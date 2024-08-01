// Package cloudslam gets and uploads the package and thumbnail.
package cloudslam

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	pbPackage "go.viam.com/api/app/packages/v1"
	"go.viam.com/rdk/services/slam"
	"google.golang.org/protobuf/types/known/structpb"
)

// UploadChunkSize defines the size of the data included in each message of a CreatePackage stream.
const (
	UploadChunkSize   = 32 * 1024
	pcdName           = "map.pcd"
	internalStateName = "internalState.pbstream"
)

// UploadPackage grabs the current pcd map and internal state of the cloud managed robot
// and creates a package at the robots location using the CreatePackage API.
func (svc *cloudslamWrapper) UploadPackage(ctx context.Context, mapName string) (string, error) {
	files, err := svc.GetMapsFromSLAM(ctx)
	if err != nil {
		return "", errors.Wrapf(err, "error getting maps")
	}

	pcd, err := findPCD(files)
	if err != nil {
		return "", err
	}

	jpeg, err := pcdToJpeg(pcd)
	if err != nil {
		return "", err
	}

	endTime := time.Now()
	packageVersion := strconv.FormatInt(endTime.Unix(), 10)

	thumbnailFileID, err := svc.uploadJpeg(ctx, jpeg, mapName, packageVersion)
	if err != nil {
		return "", err
	}
	myPackage, err := CreateArchive(files)
	if err != nil {
		return "", errors.Wrapf(err, "error creating gzip")
	}

	// setup streaming client for request
	stream, err := svc.app.PackageClient.CreatePackage(ctx)
	if err != nil {
		return "", errors.Wrapf(err, "error starting CreatePackage stream")
	}
	SLAMFile := []*pbPackage.FileInfo{}
	for _, file := range files {
		packageFile := pbPackage.FileInfo{Name: file.name, Size: uint64(len(file.contents))}
		SLAMFile = append(SLAMFile, &packageFile)
	}

	myMeta, err := svc.GetPackageMetadata(thumbnailFileID)
	if err != nil {
		return "", errors.Wrapf(err, "error creating Package Metadata")
	}
	packageInfo := pbPackage.PackageInfo{
		OrganizationId: svc.organizationID,
		Name:           mapName,
		Version:        packageVersion,
		Type:           pbPackage.PackageType_PACKAGE_TYPE_SLAM_MAP,
		Files:          SLAMFile,
		Metadata:       myMeta,
	}

	// then send the map in chunks
	var errs error
	if err := sendPackageRequests(ctx, stream, myPackage, &packageInfo); err != nil {
		errs = multierr.Combine(errs, errors.Wrapf(err, "error syncing package"))
	}

	// close the stream and receive a response when finished
	resp, err := stream.CloseAndRecv()
	if err != nil {
		errs = multierr.Combine(errs, errors.Wrapf(err, "received error response while syncing package"))
	}
	if errs != nil {
		return "", errs
	}

	packageURL := svc.app.baseURL + "/robots?name=" + mapName + "&version=" + resp.GetVersion()
	return packageURL, nil
}

// sendPackageRequests sends the package to app
func sendPackageRequests(ctx context.Context, stream pbPackage.PackageService_CreatePackageClient,
	f *bytes.Buffer, packageInfo *pbPackage.PackageInfo,
) error {
	req := &pbPackage.CreatePackageRequest{
		Package: &pbPackage.CreatePackageRequest_Info{Info: packageInfo},
	}
	// first send the metadata for the package
	if err := stream.Send(req); err != nil {
		return errors.Wrapf(err, "sending metadata")
	}

	//nolint:errcheck
	defer stream.CloseSend()
	// Loop until there is no more content to be read from file.
	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
			// Get the next UploadRequest from the file.
			uploadReq, err := getCreatePackageRequest(ctx, f)

			// EOF means we've completed successfully.
			if errors.Is(err, io.EOF) {
				return nil
			}

			if err != nil {
				return err
			}

			if err = stream.Send(uploadReq); err != nil {
				return err
			}
		}
	}
}

// getCreatePackageRequest creates a package request with a chunk of the package
func getCreatePackageRequest(ctx context.Context, f *bytes.Buffer) (*pbPackage.CreatePackageRequest, error) {
	select {
	case <-ctx.Done():
		return nil, context.Canceled
	default:
		// Get the next file data reading from file, check for an error.
		next, err := readNextFileChunk(f)
		if err != nil {
			return nil, err
		}
		// Otherwise, return an UploadRequest and no error.
		return &pbPackage.CreatePackageRequest{
			Package: &pbPackage.CreatePackageRequest_Contents{
				Contents: next,
			},
		}, nil
	}
}

// readNextFileChunk gets a chunk of data from a buffer
func readNextFileChunk(f *bytes.Buffer) ([]byte, error) {
	byteArr := make([]byte, UploadChunkSize)
	numBytesRead, err := f.Read(byteArr)
	if numBytesRead < UploadChunkSize {
		byteArr = byteArr[:numBytesRead]
	}
	if err != nil {
		return nil, err
	}
	return byteArr, nil
}

// CreateArchive creates a tar.gz with the desired set of files.
func CreateArchive(files []SLAMFile) (*bytes.Buffer, error) {
	// Create output buffer
	out := new(bytes.Buffer)

	// Create the archive and write the output to the "out" Writer
	// Create new Writers for gzip and tar
	// These writers are chained. Writing to the tar writer will
	// write to the gzip writer which in turn will write to
	// the "out" writer
	gw := gzip.NewWriter(out)
	//nolint:errcheck
	defer gw.Close()
	tw := tar.NewWriter(gw)
	//nolint:errcheck
	defer tw.Close()

	// Iterate over files and add them to the tar archive
	for _, file := range files {
		err := addToArchive(tw, file)
		if err != nil {
			return nil, errors.Wrapf(err, "adding file to archive")
		}
	}

	return out, nil
}

// addToArchive adds a file to the zip.
func addToArchive(tw *tar.Writer, file SLAMFile) error {
	// Create a tar Header from the file info
	header := &tar.Header{
		Name: file.name,
		Mode: 0o644,
		Size: int64(len(file.contents)),
	}

	// Write file header to the tar archive
	err := tw.WriteHeader(header)
	if err != nil {
		return err
	}

	// Copy file content to tar archive
	if _, err := tw.Write(file.contents); err != nil {
		return err
	}

	return nil
}

// GetMapsFromSLAM grabs the current internal state and point cloud map and writes them to the filesystem.
func (svc *cloudslamWrapper) GetMapsFromSLAM(ctx context.Context) ([]SLAMFile, error) {
	myInternalState, err := slam.InternalStateFull(ctx, svc.slamService)
	if err != nil {
		return []SLAMFile{}, errors.Wrapf(err, "getting internal state")
	}
	svc.logger.Info("internal state has this many bytes: ", len(myInternalState))

	myPCD, err := slam.PointCloudMapFull(ctx, svc.slamService, false)
	if err != nil {
		return []SLAMFile{}, errors.Wrapf(err, "getting PCD map")
	}
	svc.logger.Info("pcd map has this many bytes: ", len(myPCD))

	return []SLAMFile{
		{
			contents:     myInternalState,
			slamFileType: InternalStateType,
			name:         internalStateName,
		},
		{
			contents:     myPCD,
			slamFileType: PCDType,
			name:         pcdName,
		},
	}, nil
}

// SLAMFileType represents the type of the SLAM file.
type SLAMFileType int64

const (
	// UnknownSLAMFileType represents an unknow SLAM file type
	// is the zero value of SLAMFileType.
	UnknownSLAMFileType SLAMFileType = iota
	// PCDType represents a PCD SLAM file type.
	PCDType
	// InternalStateType represents an internal state SLAM file type.
	InternalStateType
)

// SLAMFile contains files for the SLAM package to send.
type SLAMFile struct {
	contents     []byte
	slamFileType SLAMFileType
	name         string
}

// findPCD finds a pcd file in a list of slam files
func findPCD(files []SLAMFile) ([]byte, error) {
	for _, f := range files {
		if f.slamFileType == PCDType {
			return f.contents, nil
		}
	}

	return nil, errors.New("pcd not found in slam files")
}

// GetPackageMetadata grabs the metadata for a package from the config.
func (svc *cloudslamWrapper) GetPackageMetadata(thumbnailFileID string) (*structpb.Struct, error) {
	if thumbnailFileID == "" {
		return nil, errors.New("thumbnailFileID is empty")
	}

	sensorMetadata, err := svc.ParseSensorsForPackage()
	if err != nil {
		return nil, err
	}

	myMeta, err := structpb.NewStruct(map[string]interface{}{
		"file_id":       thumbnailFileID,
		"module":        "cartographer-module",
		"moduleVersion": svc.slamVersion,
		"robot_id":      svc.robotID,
		"location_id":   svc.locationID,
		"sensors":       sensorMetadata,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "error creating package metadata")
	}
	return myMeta, nil
}
