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

	dialOpts := make([]rpc.DialOption, 0, 2)
	// Only add credentials when secret is set.

	dialOpts = append(dialOpts, rpc.WithEntityCredentials(
		svc.APIKeyID,
		rpc.Credentials{
			Type:    rpc.CredentialsTypeAPIKey,
			Payload: svc.APIKey,
		}),
	)

	conn, err := rpc.DialDirectGRPC(svc.cancelCtx, u.Host, svc.logger.AsZap(), dialOpts...)
	if err != nil {
		return err
	}

	svc.csClient = pbCloudSLAM.NewCloudSLAMServiceClient(conn)
	svc.syncClient = pbDataSync.NewDataSyncServiceClient(conn)
	svc.packageClient = pbPackage.NewPackageServiceClient(conn)
	svc.clientConn = conn
	return nil
}

// NewCloudSLAMClientFromConn creates a new CloudSLAMClient.
func NewCloudSLAMClientFromConn(conn rpc.ClientConn) pbCloudSLAM.CloudSLAMServiceClient {
	c := pbCloudSLAM.NewCloudSLAMServiceClient(conn)
	return c
}

// NewPackageClientFromConn creates a new PackageClient.
func NewPackageClientFromConn(conn rpc.ClientConn) pbPackage.PackageServiceClient {
	c := pbPackage.NewPackageServiceClient(conn)
	return c
}

// NewDataSyncClientFromConn creates a new DataSyncClient.
func NewDataSyncClientFromConn(conn rpc.ClientConn) pbDataSync.DataSyncServiceClient {
	c := pbDataSync.NewDataSyncServiceClient(conn)
	return c
}

// getDataFromHTTP makes a request to an http endpoint app serves, which gets redirected to GCS.
func (svc *cloudslamWrapper) getDataFromHTTP(ctx context.Context, dataURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, dataURL, nil)
	if err != nil {
		return nil, err
	}
	// linter wants us to use Key_id and Key
	//nolint:canonicalheader
	req.Header.Add("key_id", svc.APIKeyID)
	//nolint:canonicalheader
	req.Header.Add("key", svc.APIKey)

	res, err := svc.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	//nolint:errcheck
	defer res.Body.Close()

	return io.ReadAll(res.Body)
}
