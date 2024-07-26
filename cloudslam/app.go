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
	"go.viam.com/rdk/logging"
	"go.viam.com/utils/rpc"
)

type appClient struct {
	apiKey   string // a location owner API Key is needed to connect to app and use app related features
	apiKeyID string
	// app client fields
	baseURL       string         // defines which app to connect to(currently only prod)
	clientConn    rpc.ClientConn // connection used for the app clients
	csClient      pbCloudSLAM.CloudSLAMServiceClient
	packageClient pbPackage.PackageServiceClient
	syncClient    pbDataSync.DataSyncServiceClient
	httpClient    *http.Client // used for downloading pcds of the current cloudslam session
}

// CreateCloudSLAMClient creates a new grpc cloud configured to communicate with the robot service based on the cloud config given.
func CreateCloudSLAMClient(ctx context.Context, apiKey, apiKeyID, baseURL string, logger logging.Logger) (*appClient, error) {
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
	return &appClient{apiKey: apiKey,
		apiKeyID:      apiKeyID,
		baseURL:       baseURL,
		clientConn:    conn,
		csClient:      pbCloudSLAM.NewCloudSLAMServiceClient(conn),
		syncClient:    pbDataSync.NewDataSyncServiceClient(conn),
		packageClient: pbPackage.NewPackageServiceClient(conn),
	}, nil
}

// getDataFromHTTP makes a request to an http endpoint app serves, which gets redirected to GCS.
// will remove nolint in the next pr when this function gets used to retrieve pcds
//
//nolint:unused
func (app *appClient) getDataFromHTTP(ctx context.Context, dataURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, dataURL, nil)
	if err != nil {
		return nil, err
	}
	// linter wants us to use Key_id and Key
	//nolint:canonicalheader
	req.Header.Add("key_id", app.apiKeyID)
	//nolint:canonicalheader
	req.Header.Add("key", app.apiKey)

	res, err := app.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	//nolint:errcheck
	defer res.Body.Close()

	return io.ReadAll(res.Body)
}

func (app *appClient) Close() error {
	return app.clientConn.Close()
}
