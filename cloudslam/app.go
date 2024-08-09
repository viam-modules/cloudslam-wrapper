// Package cloudslam implements a slam service that wraps cloudslam.
package cloudslam

import (
	"context"
	"io"
	"net/http"
	"net/url"

	pbCloudSLAM "go.viam.com/api/app/cloudslam/v1"
	pbDataSync "go.viam.com/api/app/datasync/v1"
	pbPackage "go.viam.com/api/app/packages/v1"
	pbApp "go.viam.com/api/app/v1"
	"go.viam.com/rdk/logging"
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
	HTTPClient    *http.Client // used for downloading pcds of the current cloudslam session
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

// Close closes the app clients.
func (app *AppClient) Close() error {
	// close any idle connections to prevent goleaks. Possibly redundant with DisableKeepAlives
	app.HTTPClient.CloseIdleConnections()
	return app.clientConn.Close()
}
