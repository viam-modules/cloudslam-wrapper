// Package cloudslam implements a slam service that wraps cloudslam.
package cloudslam

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	pbCloudSLAM "go.viam.com/api/app/cloudslam/v1"
	pbDataSync "go.viam.com/api/app/datasync/v1"
	pbPackage "go.viam.com/api/app/packages/v1"
	"go.viam.com/rdk/grpc"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	"go.viam.com/rdk/services/slam"
	"go.viam.com/rdk/spatialmath"
	"go.viam.com/rdk/utils"
	"go.viam.com/utils/rpc"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	startJobKey      = "start"
	stopJobKey       = "stop"
	localPackageKey  = "save-local-map"
	timeFormat       = time.RFC3339
	chunkSizeBytes   = 1 * 1024 * 1024
	defaultLidarFreq = 5      // Hz
	defaultMSFreq    = 20     // Hz
	mapRefreshRate   = 1 / 5. // Hz
)

// Model is the model triplet for the cloudslam wrapper.
var Model = resource.NewModel("viam", "cloudslam-wrapper", "cloudslam")

// Config is the config for cloudslam.
type Config struct {
	APIKey               string  `json:"api_key"`
	APIKeyID             string  `json:"api_key_id"`
	SLAMService          string  `json:"slam_service"`
	RobotID              string  `json:"robot_id"`
	PartID               string  `json:"robot_part_id,omitempty"`
	LocationID           string  `json:"location_id"`
	OrganizationID       string  `json:"organization_id"`
	MovementSensorFreqHz float64 `json:"movement_sensor_freq_hz,omitempty"`
	LidarFreqHz          float64 `json:"camera_freq_hz,omitempty"`
	SLAMVersion          string  `json:"slam_version,omitempty"`
	VIAMVersion          string  `json:"viam_version,omitempty"`
}

type cloudslamWrapper struct {
	resource.Named
	resource.AlwaysRebuild

	mu        sync.RWMutex
	activeJob string
	// muPos     sync.RWMutex
	lastPos spatialmath.Pose
	// muPCD     sync.RWMutex
	// lastPCD   string

	slamService slam.Service           // the slam service that cloudslam will wrap
	sensors     []*cloudslamSensorInfo // sensors currently in use by the slam service
	apiKey      string                 // an API Key is needed to connect to app and use app related features. must be a location owner or greater
	apiKeyID    string
	// these define which robot/location/org we want to upload the map to. the API key should be defined for this location/org
	robotID        string
	partID         string
	locationID     string
	organizationID string
	viamVersion    string // optional cloudslam setting, describes which viam-server appimage to use(stable/latest/pr/pinned)
	slamVersion    string // optional cloudslam setting, describes which cartographer appimage to use(stable/latest/pr/pinned)
	msFreq         float64
	lidarFreq      float64
	defaultpcd     []byte

	// app client fields
	baseURL       string         // defines which app to connect to(currently only prod)
	clientConn    rpc.ClientConn // connection used for the app clients
	csClient      pbCloudSLAM.CloudSLAMServiceClient
	packageClient pbPackage.PackageServiceClient
	syncClient    pbDataSync.DataSyncServiceClient
	httpClient    *http.Client // used for downloading pcds of the current cloudslam session

	workers    utils.StoppableWorkers
	logger     logging.Logger
	cancelCtx  context.Context
	cancelFunc func()
}

type cloudslamSensorInfo struct {
	name       string
	sensorType slam.SensorType
	freq       float64
}

func init() {
	resource.RegisterService(
		slam.API,
		Model,
		resource.Registration[slam.Service, *Config]{Constructor: newSLAM})
}

// Validate validates the config for cloudslam.
func (cfg *Config) Validate(path string) ([]string, error) {
	// resource.NewConfigValidationFieldRequiredError(path, "i2c_bus")
	if cfg.SLAMService == "" {
		return []string{}, resource.NewConfigValidationFieldRequiredError(path, "slam_service")
	}
	if cfg.APIKey == "" {
		return []string{}, resource.NewConfigValidationFieldRequiredError(path, "api_key")
	}
	if cfg.APIKeyID == "" {
		return []string{}, resource.NewConfigValidationFieldRequiredError(path, "api_key_id")
	}
	if cfg.RobotID == "" {
		return []string{}, resource.NewConfigValidationFieldRequiredError(path, "robot_id")
	}
	if cfg.LocationID == "" {
		return []string{}, resource.NewConfigValidationFieldRequiredError(path, "location_id")
	}
	if cfg.OrganizationID == "" {
		return []string{}, resource.NewConfigValidationFieldRequiredError(path, "organization_id")
	}
	return []string{cfg.SLAMService}, nil
}

func newSLAM(
	ctx context.Context,
	deps resource.Dependencies,
	conf resource.Config,
	logger logging.Logger,
) (slam.Service, error) {
	newConf, err := resource.NativeConfig[*Config](conf)
	if err != nil {
		return nil, err
	}
	wrappedSLAM, err := slam.FromDependencies(deps, newConf.SLAMService)
	if err != nil {
		return nil, err
	}

	viamVersion := newConf.VIAMVersion
	if viamVersion == "" {
		viamVersion = "stable"
	}
	slamVersion := newConf.SLAMVersion
	if slamVersion == "" {
		slamVersion = "stable"
	}
	lidarFreq := newConf.LidarFreqHz
	if lidarFreq == 0 {
		lidarFreq = defaultLidarFreq
	}
	msFreq := newConf.MovementSensorFreqHz
	if msFreq == 0 {
		msFreq = defaultMSFreq
	}

	props, err := wrappedSLAM.Properties(ctx)
	if err != nil {
		return nil, err
	}
	csSensors := sensorInfoToCSSensorInfo(props.SensorInfo, msFreq, lidarFreq)

	cancelCtx, cancel := context.WithCancel(context.Background())
	wrapper := &cloudslamWrapper{
		Named:          conf.ResourceName().AsNamed(),
		baseURL:        "https://app.viam.com",
		apiKey:         newConf.APIKey,
		apiKeyID:       newConf.APIKeyID,
		slamService:    wrappedSLAM,
		viamVersion:    viamVersion,
		slamVersion:    slamVersion,
		msFreq:         msFreq,
		lidarFreq:      lidarFreq,
		logger:         logger,
		cancelCtx:      cancelCtx,
		cancelFunc:     cancel,
		robotID:        newConf.RobotID,
		locationID:     newConf.LocationID,
		organizationID: newConf.OrganizationID,
		partID:         newConf.PartID,
		httpClient:     &http.Client{},
		lastPos:        spatialmath.NewZeroPose(),
		sensors:        csSensors,
	}

	// using this as a placeholder image. need to determine the right way to have the module use it
	path := filepath.Clean("./test2.pcd")
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	wrapper.defaultpcd = bytes

	err = wrapper.CreateCloudSLAMClient()
	if err != nil {
		return nil, err
	}

	// wrapper.activeMappingSessionThread()

	return wrapper, nil
}

func (svc *cloudslamWrapper) Position(ctx context.Context) (spatialmath.Pose, error) {
	// adding the lock once we start requesting positions from app(future pr)
	// svc.muPos.RLock()
	currPos := svc.lastPos
	// svc.muPos.RUnlock()

	return currPos, nil
}

func (svc *cloudslamWrapper) PointCloudMap(ctx context.Context, returnEditedMap bool) (func() ([]byte, error), error) {
	// return the placeholder map when no maps are present
	return toChunkedFunc(svc.defaultpcd), nil
}

func (svc *cloudslamWrapper) InternalState(ctx context.Context) (func() ([]byte, error), error) {
	return nil, grpc.UnimplementedError
}

func (svc *cloudslamWrapper) Properties(ctx context.Context) (slam.Properties, error) {
	return slam.Properties{MappingMode: slam.MappingModeNewMap}, nil
}

func (svc *cloudslamWrapper) Close(ctx context.Context) error {
	svc.cancelFunc()
	if svc.workers != nil {
		svc.workers.Stop()
	}
	if svc.clientConn != nil {
		return svc.clientConn.Close()
	}
	return nil
}

func (svc *cloudslamWrapper) DoCommand(ctx context.Context, req map[string]interface{}) (map[string]interface{}, error) {
	resp := map[string]interface{}{}
	if name, ok := req[startJobKey]; ok {
		jobID, err := svc.StartJob(svc.cancelCtx, name.(string))
		if err != nil {
			return nil, err
		}
		svc.mu.Lock()
		svc.activeJob = jobID
		svc.mu.Unlock()
		// svc.muPos.Lock()
		// svc.lastPos = spatialmath.NewZeroPose()
		// svc.muPos.Unlock()
		// svc.muPCD.Lock()
		// svc.lastPCD = ""
		// svc.muPCD.Unlock()
		resp[startJobKey] = "Starting cloudslam session, the robot should appear in ~1 minute. Job ID: " + jobID
	}
	if _, ok := req[stopJobKey]; ok {
		packageURL, err := svc.StopJob(ctx)
		if err != nil {
			return nil, err
		}
		resp[stopJobKey] = "Job completed, find your map at " + packageURL
	}
	return resp, nil
}

// StopJob stops the current active cloudslam job.
func (svc *cloudslamWrapper) StopJob(ctx context.Context) (string, error) {
	// grab the active job but do not clear it from the module. that way users can still see the final map on the robot
	svc.mu.RLock()
	currJob := svc.activeJob
	svc.mu.RUnlock()
	if currJob == "" {
		return "", errors.New("no active jobs")
	}

	req := &pbCloudSLAM.StopMappingSessionRequest{SessionId: currJob}
	resp, err := svc.csClient.StopMappingSession(ctx, req)
	if err != nil {
		return "", err
	}
	packageName := strings.Split(resp.GetPackageId(), "/")[1]
	packageURL := svc.baseURL + "/robots?name=" + packageName + "&version=" + resp.GetVersion()
	return packageURL, nil
}

// StartJob starts a cloudslam job with the requested map name. Currently assumes a set of config parameters.
func (svc *cloudslamWrapper) StartJob(ctx context.Context, mapName string) (string, error) {
	starttime := timestamppb.New(time.Now())
	interval := pbCloudSLAM.CaptureInterval{StartTime: starttime}
	configParams, err := structpb.NewStruct(map[string]any{
		"attributes": map[string]any{
			"config_params": map[string]any{
				"mode":             "2d",
				"min_range_meters": "0.2",
				"max_range_meters": "25",
			},
		},
	})
	if err != nil {
		return "", err
	}
	req := &pbCloudSLAM.StartMappingSessionRequest{
		SlamVersion: svc.slamVersion, ViamServerVersion: svc.viamVersion, MapName: mapName, OrganizationId: svc.organizationID,
		LocationId: svc.locationID, RobotId: svc.robotID, CaptureInterval: &interval, Sensors: svc.sensorInfoToProto(), SlamConfig: configParams,
	}
	resp, err := svc.csClient.StartMappingSession(ctx, req)
	if err != nil {
		return "", err
	}
	return resp.GetSessionId(), nil
}

func sensorInfoToCSSensorInfo(sensors []slam.SensorInfo, msFreq, cameraFreq float64) []*cloudslamSensorInfo {
	sensorsCS := []*cloudslamSensorInfo{}
	for _, sensor := range sensors {
		var freq float64
		if sensor.Type.String() == slam.SensorTypeCamera.String() {
			freq = cameraFreq
		} else {
			freq = msFreq
		}
		sensorsCS = append(sensorsCS, &cloudslamSensorInfo{name: sensor.Name, sensorType: sensor.Type, freq: freq})
	}
	return sensorsCS
}

func (svc *cloudslamWrapper) sensorInfoToProto() []*pbCloudSLAM.SensorInfo {
	sensorsProto := []*pbCloudSLAM.SensorInfo{}
	for _, sensor := range svc.sensors {
		sensorsProto = append(sensorsProto,
			&pbCloudSLAM.SensorInfo{
				SourceComponentName: sensor.name,
				Type:                sensor.sensorType.String(),
				DataFrequencyHz:     strconv.FormatFloat(sensor.freq, 'f', -1, 64),
			})
	}
	return sensorsProto
}

// toChunkedFunc takes binary data and wraps it in a helper function that converts it into chunks for streaming APIs.
func toChunkedFunc(b []byte) func() ([]byte, error) {
	chunk := make([]byte, chunkSizeBytes)

	reader := bytes.NewReader(b)

	f := func() ([]byte, error) {
		bytesRead, err := reader.Read(chunk)
		if err != nil {
			return nil, err
		}
		return chunk[:bytesRead], err
	}
	return f
}
