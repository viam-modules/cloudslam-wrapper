// Package cloudslam implements a slam service that wraps cloudslam.
package cloudslam

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	pbCloudSLAM "go.viam.com/api/app/cloudslam/v1"
	"go.viam.com/rdk/grpc"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	"go.viam.com/rdk/services/slam"
	"go.viam.com/rdk/spatialmath"
	"go.viam.com/rdk/utils"
	goutils "go.viam.com/utils"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	startJobKey      = "start"
	stopJobKey       = "stop"
	localPackageKey  = "save-local-map"
	timeFormat       = time.RFC3339
	chunkSizeBytes   = 1 * 1024 * 1024
	defaultLidarFreq = 5   // Hz
	defaultMSFreq    = 20  // Hz
	mapRefreshRate   = 5.0 // Seconds
)

var (
	initPose   = spatialmath.NewZeroPose()
	initPCDURL = ""

	// Model is the model triplet for the cloudslam wrapper.
	Model = resource.NewModel("viam", "cloudslam-wrapper", "cloudslam")
)

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

	activeJob         atomic.Pointer[string]
	lastPose          atomic.Pointer[spatialmath.Pose]
	lastPointCloudURL atomic.Pointer[string]

	slamService slam.Service           // the slam service that cloudslam will wrap
	sensors     []*cloudslamSensorInfo // sensors currently in use by the slam service

	// these define which robot/location/org we want to upload the map to. the API key should be defined for this location/org
	robotID        string
	partID         string
	locationID     string
	organizationID string
	viamVersion    string // optional cloudslam setting, describes which viam-server appimage to use(stable/latest/pr/pinned)
	slamVersion    string // optional cloudslam setting, describes which cartographer appimage to use(stable/latest/pr/pinned)
	defaultpcd     []byte

	// app clients for talking to app
	app *AppClient

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

	appClients, err := CreateCloudSLAMClient(cancelCtx, newConf.APIKey, newConf.APIKeyID, "https://app.viam.com", logger)
	if err != nil {
		cancel()
		return nil, err
	}

	wrapper := &cloudslamWrapper{
		Named:          conf.ResourceName().AsNamed(),
		slamService:    wrappedSLAM,
		viamVersion:    viamVersion,
		slamVersion:    slamVersion,
		logger:         logger,
		cancelCtx:      cancelCtx,
		cancelFunc:     cancel,
		robotID:        newConf.RobotID,
		locationID:     newConf.LocationID,
		organizationID: newConf.OrganizationID,
		partID:         newConf.PartID,
		sensors:        csSensors,
		app:            appClients,
	}
	wrapper.lastPose.Store(&initPose)
	initJob := ""
	wrapper.activeJob.Store(&initJob)
	wrapper.lastPointCloudURL.Store(&initPCDURL)

	// using this as a placeholder image. need to determine the right way to have the module use it
	path := filepath.Clean("./test2.pcd")
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	wrapper.defaultpcd = bytes

	// check if the robot has an active job
	reqActives := &pbCloudSLAM.GetActiveMappingSessionsForRobotRequest{RobotId: wrapper.robotID}
	resp, err := appClients.CSClient.GetActiveMappingSessionsForRobot(cancelCtx, reqActives)
	if err != nil {
		return nil, err
	}
	wrapper.activeJob.Store(&resp.SessionId)

	wrapper.workers = utils.NewStoppableWorkers(wrapper.activeMappingSessionThread)

	return wrapper, nil
}

func (svc *cloudslamWrapper) activeMappingSessionThread(ctx context.Context) {
	for {
		if !goutils.SelectContextOrWait(ctx, time.Duration(1000.*mapRefreshRate)*time.Millisecond) {
			return
		}

		currJob := *svc.activeJob.Load()
		// do nothing if no active jobs
		if currJob == "" {
			continue
		}

		// get the most recent pointcloud and position if there is an active job
		req := &pbCloudSLAM.GetMappingSessionPointCloudRequest{SessionId: currJob}
		resp, err := svc.app.CSClient.GetMappingSessionPointCloud(ctx, req)
		if err != nil {
			svc.logger.Error(err)
			continue
		}

		currPose := spatialmath.NewPoseFromProtobuf(resp.GetPose())

		svc.lastPose.Store(&currPose)
		svc.lastPointCloudURL.Store(&resp.MapUrl)
	}
}

func (svc *cloudslamWrapper) Position(ctx context.Context) (spatialmath.Pose, error) {
	return *svc.lastPose.Load(), nil
}

func (svc *cloudslamWrapper) PointCloudMap(ctx context.Context, returnEditedMap bool) (func() ([]byte, error), error) {
	currMap := *svc.lastPointCloudURL.Load()

	// return the placeholder map when no maps are present
	if currMap == "" {
		return toChunkedFunc(svc.defaultpcd), nil
	}
	pcdBytes, err := svc.app.GetDataFromHTTP(ctx, currMap)
	if err != nil {
		return nil, err
	}
	return toChunkedFunc(pcdBytes), nil
}

func (svc *cloudslamWrapper) InternalState(ctx context.Context) (func() ([]byte, error), error) {
	return nil, grpc.UnimplementedError
}

func (svc *cloudslamWrapper) Properties(ctx context.Context) (slam.Properties, error) {
	return slam.Properties{MappingMode: slam.MappingModeNewMap}, nil
}

func (svc *cloudslamWrapper) Close(ctx context.Context) error {
	svc.cancelFunc()
	svc.workers.Stop()
	return svc.app.Close()
}

func (svc *cloudslamWrapper) DoCommand(ctx context.Context, req map[string]interface{}) (map[string]interface{}, error) {
	resp := map[string]interface{}{}
	if name, ok := req[startJobKey]; ok {
		jobID, err := svc.StartJob(svc.cancelCtx, name.(string))
		if err != nil {
			return nil, err
		}
		svc.activeJob.Store(&jobID)
		svc.lastPose.Store(&initPose)
		svc.lastPointCloudURL.Store(&initPCDURL)

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
	currJob := *svc.activeJob.Load()
	if currJob == "" {
		return "", errors.New("no active jobs")
	}

	req := &pbCloudSLAM.StopMappingSessionRequest{SessionId: currJob}
	resp, err := svc.app.CSClient.StopMappingSession(ctx, req)
	if err != nil {
		return "", err
	}
	packageName := strings.Split(resp.GetPackageId(), "/")[1]
	packageURL := svc.app.baseURL + "/robots?name=" + packageName + "&version=" + resp.GetVersion()
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
	resp, err := svc.app.CSClient.StartMappingSession(ctx, req)
	if err != nil {
		return "", err
	}
	return resp.GetSessionId(), nil
}

// sensorInfoToCSSensorInfo takes in a set of sensors from a SLAM algorithm's properties and adds each sensor's frequency to them
// We have to do this because the current SLAM APIs do not include the sensor's configured frequency.
// This currently assumes all configured cameras and movement sensors will use the same frequency as well,
// and that no other sensor types will be used
// the cloudslamSensorInfo struct is used for both package creation and cloudslam.
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

// takes a set of slam sensors and converts them into proto messages for cloudslam.
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
