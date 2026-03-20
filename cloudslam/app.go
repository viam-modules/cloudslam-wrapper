// Package cloudslam implements a slam service that wraps cloudslam.
package cloudslam

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	pbCloudSLAM "go.viam.com/api/app/cloudslam/v1"
	pbDataSync "go.viam.com/api/app/datasync/v1"
	pbPackage "go.viam.com/api/app/packages/v1"
	pbApp "go.viam.com/api/app/v1"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/services/slam"
	"go.viam.com/utils/rpc"
)

// AppClient contains all of the client connections to app.
type AppClient struct {
	apiKey   string // a location owner API Key is needed to connect to app and use app related features
	apiKeyID string
	// app client fields
	baseURL       string         // defines which app to connect to(currently only prod)
	clientConn    rpc.ClientConn // connection used for the app clients
	CSClient      pbCloudSLAM.CloudSLAMServiceClient
	PackageClient pbPackage.PackageServiceClient
	SyncClient    pbDataSync.DataSyncServiceClient
	RobotClient   pbApp.RobotServiceClient
	HTTPClient    *http.Client   // used for downloading pcds of the current cloudslam session
	logger        logging.Logger
}

// CreateCloudSLAMClient creates a new grpc cloud configured to communicate with the robot service based on the cloud config given.
func CreateCloudSLAMClient(ctx context.Context, apiKey, apiKeyID, baseURL string, logger logging.Logger) (*AppClient, error) {
	u, err := url.Parse(baseURL + ":443")
	if err != nil {
		return nil, err
	}

	opts := rpc.WithEntityCredentials(
		apiKeyID,
		rpc.Credentials{
			Type:    rpc.CredentialsTypeAPIKey,
			Payload: apiKey,
		})

	conn, err := rpc.DialDirectGRPC(ctx, u.Host, logger.AsZap(), opts)
	if err != nil {
		return nil, err
	}
	return &AppClient{
		apiKey:        apiKey,
		apiKeyID:      apiKeyID,
		baseURL:       baseURL,
		clientConn:    conn,
		CSClient:      pbCloudSLAM.NewCloudSLAMServiceClient(conn),
		SyncClient:    pbDataSync.NewDataSyncServiceClient(conn),
		PackageClient: pbPackage.NewPackageServiceClient(conn),
		RobotClient:   pbApp.NewRobotServiceClient(conn),
		// Disable keepalives makes each request only last for a single http GET request.
		//  Doing this to prevent any active connections from causing goleaks when the viam-server shuts down.
		// This might be redundant with CloseIdleConnections in Close(),
		// and unsure if the extra cost of redoing the TLS handshake makes this change worth it
		HTTPClient: &http.Client{Transport: &http.Transport{DisableKeepAlives: true}},
		logger:     logger,
	}, nil
}

// GetSLAMMapPackageOnRobot makes a Config request to app and returns the first SLAM map that it finds on the robot.
func (app *AppClient) GetSLAMMapPackageOnRobot(ctx context.Context, partID string) (string, string, error) {
	req := pbApp.ConfigRequest{Id: partID}
	resp, err := app.RobotClient.Config(ctx, &req)
	if err != nil {
		return "", "", err
	}
	packages := resp.GetConfig().GetPackages()
	for _, robotPackage := range packages {
		if robotPackage.GetType() == "slam_map" {
			return robotPackage.GetName(), robotPackage.GetVersion(), nil
		}
	}
	return "", "", nil
}

// GetDataFromHTTP makes a request to an http endpoint app serves, which gets redirected to GCS.
func (app *AppClient) GetDataFromHTTP(ctx context.Context, dataURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, dataURL, nil)
	if err != nil {
		return nil, err
	}
	// linter wants us to use Key_id and Key
	//nolint:canonicalheader
	req.Header.Add("key_id", app.apiKeyID)
	//nolint:canonicalheader
	req.Header.Add("key", app.apiKey)
	res, err := app.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	//nolint:errcheck
	defer res.Body.Close()

	return io.ReadAll(res.Body)
}

// CheckSensorsDataCapture verifies that all of the provided sensors have at least one enabled
// data capture method configured in the machine part's config. Returns an error listing any sensors
// that are missing enabled capture.
func (app *AppClient) CheckSensorsDataCapture(ctx context.Context, partID string, sensors []*cloudslamSensorInfo) error {
	req := pbApp.ConfigRequest{Id: partID}
	resp, err := app.RobotClient.Config(ctx, &req)
	if err != nil {
		return err
	}

	pending := make(map[string]struct{}, len(sensors))
	sensorTypes := make(map[string]slam.SensorType, len(sensors))
	for _, s := range sensors {
		pending[s.name] = struct{}{}
		sensorTypes[s.name] = s.sensorType
	}

	for _, comp := range resp.GetConfig().GetComponents() {
		if _, ok := pending[comp.GetName()]; !ok {
			continue
		}
		app.logger.Debugf("checking data capture for sensor %q (type %v)", comp.GetName(), sensorTypes[comp.GetName()])
		for _, svcConfig := range comp.GetServiceConfigs() {
			app.logger.Debugf("  service config type: %q, attributes: %v", svcConfig.GetType(), svcConfig.GetAttributes())
		}
		if hasEnabledDataCapture(comp, sensorTypes[comp.GetName()]) {
			delete(pending, comp.GetName())
		}
	}

	if len(pending) > 0 {
		missing := make([]string, 0, len(pending))
		for name := range pending {
			missing = append(missing, name)
		}
		return fmt.Errorf("the following sensors do not have data capture enabled: %v", missing)
	}
	return nil
}

// hasEnabledDataCapture returns true if the component has an appropriate enabled capture method
// in its data_manager service config. For cameras, NextPointCloud must be configured and enabled.
// For other sensor types, any enabled capture method is sufficient.
func hasEnabledDataCapture(comp *pbApp.ComponentConfig, sensorType slam.SensorType) bool {
	for _, svcConfig := range comp.GetServiceConfigs() {
		if svcConfig.GetType() != "rdk:service:data_manager" {
			continue
		}
		captureMethods := svcConfig.GetAttributes().GetFields()["capture_methods"]
		if captureMethods == nil {
			continue
		}
		for _, method := range captureMethods.GetListValue().GetValues() {
			fields := method.GetStructValue().GetFields()
			if fields["disabled"].GetBoolValue() {
				continue
			}
			if sensorType == slam.SensorTypeCamera {
				if fields["method"].GetStringValue() == "NextPointCloud" {
					return true
				}
			} else {
				return true
			}
		}
	}
	return false
}

// Close closes the app clients.
func (app *AppClient) Close() error {
	// close any idle connections to prevent goleaks. Possibly redundant with DisableKeepAlives
	app.HTTPClient.CloseIdleConnections()
	return app.clientConn.Close()
}
