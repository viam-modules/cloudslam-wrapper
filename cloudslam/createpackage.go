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

// UploadPackage grabs the current pcd map and internal state of the cloud managed machine
// and creates a package at the machines location using the CreatePackage API.
func (svc *cloudslamWrapper) UploadPackage(ctx context.Context, mapName string) (string, error) {
	if svc.partID == "" {
		return "", errors.New("must set machine_part_id in config to use this feature")
	}
	// grab the current time to mark the "version" of the slam map
	packageVersion := strconv.FormatInt(time.Now().Unix(), 10)

	files, err := svc.GetMapsFromSLAM(ctx)
	if err != nil {
		return "", errors.Wrapf(err, "error getting maps")
	}

	// see thumbnail.go for all of this code
	thumbnailFileID, err := svc.createThumbnail(ctx, files, mapName, packageVersion)
	if err != nil {
		return "", err
	}

	// if any errors occur after this point, we will have an orphaned thumbnail uploaded to app.
	// this is an acceptable behavior, but if we wanted to delete them then we need to add a data management client
	// and use the thumbnailFileID to delete the thumbnail

	err = svc.uploadArchive(ctx, files, mapName, thumbnailFileID, packageVersion)
	if err != nil {
		return "", err
	}

	// return a link for where to find the package
	packageURL := svc.app.baseURL + "/robots?name=" + mapName + "&version=" + packageVersion
	return packageURL, nil
}

// uploadArchive creates a tar/archive of the SLAM map and uploads it to app using the package APIs.
func (svc *cloudslamWrapper) uploadArchive(ctx context.Context, files []SLAMFile, mapName, thumbnailFileID, packageVersion string) error {
	myPackage, err := CreateArchive(files)
	if err != nil {
		return errors.Wrapf(err, "error creating gzip")
	}

	// setup streaming client for request
	stream, err := svc.app.PackageClient.CreatePackage(ctx)
	if err != nil {
		return errors.Wrapf(err, "error starting CreatePackage stream")
	}
	SLAMFile := []*pbPackage.FileInfo{}
	for _, file := range files {
		packageFile := pbPackage.FileInfo{Name: file.name, Size: uint64(len(file.contents))}
		SLAMFile = append(SLAMFile, &packageFile)
	}

	myMeta, err := svc.GetPackageMetadata(thumbnailFileID)
	if err != nil {
		return errors.Wrapf(err, "error creating Package Metadata")
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
	_, err = stream.CloseAndRecv()
	if err != nil {
		errs = multierr.Combine(errs, errors.Wrapf(err, "received error response while syncing package"))
	}
	if errs != nil {
		return errs
	}
	return nil
}

// sendPackageRequests sends the package to app.
func sendPackageRequests(ctx context.Context, stream pbPackage.PackageService_CreatePackageClient,
	f *bytes.Buffer, packageInfo *pbPackage.PackageInfo,
) error {
	var err error
	req := &pbPackage.CreatePackageRequest{
		Package: &pbPackage.CreatePackageRequest_Info{Info: packageInfo},
	}
	// first send the metadata for the package
	if err = stream.Send(req); err != nil {
		return errors.Wrapf(err, "sending metadata")
	}
	defer func() { err = multierr.Combine(err, stream.CloseSend()) }()

	// Loop until there is no more content to be read from file.
	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
			// Get the next CreatePackageRequest from the file.
			createPackageReq, err := getCreatePackageRequest(ctx, f)

			// EOF means we've completed successfully.
			if errors.Is(err, io.EOF) {
				return nil
			}

			if err != nil {
				return err
			}

			if err = stream.Send(createPackageReq); err != nil {
				return err
			}
		}
	}
}

// getCreatePackageRequest creates a package request with a chunk of the package.
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
		// Otherwise, return an CreatePackageRequest and no error.
		return &pbPackage.CreatePackageRequest{
			Package: &pbPackage.CreatePackageRequest_Contents{
				Contents: next,
			},
		}, nil
	}
}

// readNextFileChunk gets a chunk of data from a buffer.
func readNextFileChunk(f *bytes.Buffer) ([]byte, error) {
	byteArr := make([]byte, UploadChunkSize)
	numBytesRead, err := f.Read(byteArr)
	if err != nil {
		return nil, err
	}
	if numBytesRead < UploadChunkSize {
		byteArr = byteArr[:numBytesRead]
	}

	return byteArr, nil
}

// CreateArchive creates a tar.gz with the desired set of files.
func CreateArchive(files []SLAMFile) (*bytes.Buffer, error) {
	var err error
	// Create output buffer
	out := new(bytes.Buffer)

	// Create the archive and write the output to the "out" Writer
	// Create new Writers for gzip and tar
	// These writers are chained. Writing to the tar writer will
	// write to the gzip writer which in turn will write to
	// the "out" writer
	gw := gzip.NewWriter(out)
	defer func() { err = multierr.Combine(err, gw.Close()) }()
	tw := tar.NewWriter(gw)
	defer func() { err = multierr.Combine(err, tw.Close()) }()

	// Iterate over files and add them to the tar archive
	for _, file := range files {
		err = addToArchive(tw, file)
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

// findPCD finds a pcd file in a list of slam files.
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
		"robot_id":      svc.machineID,
		"location_id":   svc.locationID,
		"sensors":       sensorMetadata,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "error creating package metadata")
	}
	return myMeta, nil
}
