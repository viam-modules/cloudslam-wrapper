// Package cloudslam implements a slam service that wraps cloudslam.
package cloudslam

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
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
	"go.viam.com/utils/rpc"
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

// Model is the model triplet for the cloudslam wrapper. viam:cloudslam-wrapper:cloudslam
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

	logger         logging.Logger
	cancelCtx      context.Context
	cancelFunc     func()
	slamService    slam.Service
	APIKey         string
	APIKeyID       string
	clientConn     rpc.ClientConn
	VIAMVersion    string
	SLAMVersion    string
	RobotID        string
	PartID         string
	LocationID     string
	OrganizationID string
	MSFreq         float64
	LidarFreq      float64
	defaultpcd     []byte
	httpClient     *http.Client
	baseURL        string

	csClient      pbCloudSLAM.CloudSLAMServiceClient
	packageClient pbPackage.PackageServiceClient
	syncClient    pbDataSync.DataSyncServiceClient

	activeBackgroundWorkers sync.WaitGroup
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

	cancelCtx, cancel := context.WithCancel(context.Background())
	wrapper := &cloudslamWrapper{
		baseURL: "https://app.viam.com", APIKey: newConf.APIKey, APIKeyID: newConf.APIKeyID,
		slamService: wrappedSLAM, VIAMVersion: viamVersion, MSFreq: msFreq, LidarFreq: lidarFreq,
		SLAMVersion: slamVersion, logger: logger, cancelCtx: cancelCtx, cancelFunc: cancel,
		RobotID: newConf.RobotID, LocationID: newConf.LocationID, OrganizationID: newConf.OrganizationID, PartID: newConf.PartID,
		httpClient: &http.Client{},
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

	// wrapper.activeBackgroundWorkers.Add(1)
	// utils.PanicCapturingGo(wrapper.activeMappingSessionThread)

	return wrapper, nil
}

func (svc *cloudslamWrapper) Position(ctx context.Context) (spatialmath.Pose, error) {

	return nil, grpc.UnimplementedError
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
	svc.activeBackgroundWorkers.Wait()
	if svc.clientConn != nil {
		return svc.clientConn.Close()
	}
	return nil
}

func (svc *cloudslamWrapper) DoCommand(ctx context.Context, req map[string]interface{}) (map[string]interface{}, error) {
	resp := map[string]interface{}{}
	return resp, nil
}

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
