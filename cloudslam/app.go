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
	"go.viam.com/utils/rpc"
)

// CreateCloudSLAMClient creates a new grpc cloud configured to communicate with the robot service based on the cloud config given.
func (svc *cloudslamWrapper) CreateCloudSLAMClient() error {
	u, err := url.Parse(svc.baseURL + ":443")
	if err != nil {
		return err
	}

	opts := rpc.WithEntityCredentials(
		svc.apiKeyID,
		rpc.Credentials{
			Type:    rpc.CredentialsTypeAPIKey,
			Payload: svc.apiKey,
		})

	conn, err := rpc.DialDirectGRPC(svc.cancelCtx, u.Host, svc.logger.AsZap(), opts)
	if err != nil {
		return err
	}

	svc.csClient = pbCloudSLAM.NewCloudSLAMServiceClient(conn)
	svc.syncClient = pbDataSync.NewDataSyncServiceClient(conn)
	svc.packageClient = pbPackage.NewPackageServiceClient(conn)
	svc.clientConn = conn
	return nil
}

// getDataFromHTTP makes a request to an http endpoint app serves, which gets redirected to GCS.
// will remove nolint in the next pr when this function gets used to retrieve pcds
//
//nolint:unused
func (svc *cloudslamWrapper) getDataFromHTTP(ctx context.Context, dataURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, dataURL, nil)
	if err != nil {
		return nil, err
	}
	// linter wants us to use Key_id and Key
	//nolint:canonicalheader
	req.Header.Add("key_id", svc.apiKeyID)
	//nolint:canonicalheader
	req.Header.Add("key", svc.apiKey)

	res, err := svc.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	//nolint:errcheck
	defer res.Body.Close()

	return io.ReadAll(res.Body)
}
