package cloudslam

import (
	"bytes"
	"context"
	"image"
	"image/jpeg"
	"io"
	"math"
	"path/filepath"

	"github.com/golang/geo/r3"
	"github.com/montanaflynn/stats"
	"github.com/pkg/errors"
	pbDataSync "go.viam.com/api/app/datasync/v1"
	"go.viam.com/rdk/pointcloud"
	"go.viam.com/rdk/rimage"
	"go.viam.com/rdk/spatialmath"
)

const (
	sigmaLevel        = 7    // level of precision for stdev calculation (determined through experimentation)
	imageHeight       = 1080 // image height
	imageWidth        = 1080 // image width
	missThreshold     = 0.49 // probability limit below which the associated point is assumed to be free space
	hitThreshold      = 0.55 // probability limit above which the associated point is assumed to indicate an obstacle
	pointRadius       = 1    // radius of pointcloud point
	robotMarkerRadius = 5    // radius of robot marker point
)

func (svc *cloudslamWrapper) uploadJpeg(
	ctx context.Context,
	content *bytes.Buffer,
	mapName string,
	versionID string,
) (string, error) {
	stream, err := svc.app.SyncClient.FileUpload(ctx)
	if err != nil {
		return "", err
	}
	name := "preview.jpeg"
	packageID := svc.organizationID + "/" + mapName

	md := &pbDataSync.UploadMetadata{
		// NOTE: Passing the PartID is temp.
		// Once we move to use Org Keys for authenticating with App
		// the PartID field can be removed in favor of sending the
		// OrgKey
		PartId:        svc.partID,
		Type:          pbDataSync.DataType_DATA_TYPE_FILE,
		FileName:      filepath.Base(name),
		FileExtension: filepath.Ext(name),
		Tags: []string{
			"VIAM_SLAM_DATA",
			"VIAM_SLAM_THUMBNAIL",
			"VIAM_SLAM_PACKAGE_ID:" + packageID,
			"VIAM_SLAM_PACKAGE_VERSION:" + versionID,
		},
	}

	// Send metadata FileUploadRequest.
	req := &pbDataSync.FileUploadRequest{
		UploadPacket: &pbDataSync.FileUploadRequest_Metadata{
			Metadata: md,
		},
	}
	if err := stream.Send(req); err != nil {
		return "", err
	}

	if err := sendFileUploadRequests(ctx, stream, content); err != nil {
		return "", errors.Wrap(err, "error syncing thumbnail")
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		return "", errors.Wrap(err, "received error response while syncing thumbnail")
	}

	return res.GetFileId(), nil
}

func pcdToJpeg(pcd []byte) (*bytes.Buffer, error) {
	ppRM := NewParallelProjectionOntoXYWithRobotMarker(nil)
	pc, err := pointcloud.ReadPCD(bytes.NewBuffer(pcd))
	if err != nil {
		return nil, errors.Wrapf(err, "converting pcd to pointcloud")
	}
	im, err := ppRM.PointCloudToRGBD(pc)
	if err != nil {
		return nil, errors.Wrapf(err, "converting pointcloud to image")
	}
	var thumbnail bytes.Buffer
	err = jpeg.Encode(&thumbnail, im, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "encoding image to jpeg")
	}
	return &thumbnail, nil
}

// ParallelProjectionOntoXYWithRobotMarker allows the creation of a 2D projection of a pointcloud and robot
// position onto the XY plane.
type ParallelProjectionOntoXYWithRobotMarker struct {
	robotPose *spatialmath.Pose
}

// PointCloudToRGBD creates an image of a pointcloud in the XY plane, scaling the points to a standard image
// size. It will also add a red marker to the map to represent the location of the robot. The returned depthMap
// is unused and so will always be nil.
func (ppRM *ParallelProjectionOntoXYWithRobotMarker) PointCloudToRGBD(cloud pointcloud.PointCloud,
) (*rimage.Image, error) {
	meta := cloud.MetaData()

	if cloud.Size() == 0 {
		return nil, errors.New("projection point cloud is empty")
	}

	meanStdevX, meanStdevY, err := calculatePointCloudMeanAndStdevXY(cloud)
	if err != nil {
		return nil, err
	}

	minX := math.Max(meanStdevX.mean-float64(sigmaLevel)*meanStdevX.stdev, meta.MinX) - pointRadius
	maxX := math.Min(meanStdevX.mean+float64(sigmaLevel)*meanStdevX.stdev, meta.MaxX) + pointRadius

	minY := math.Max(meanStdevY.mean-float64(sigmaLevel)*meanStdevY.stdev, meta.MinY) - pointRadius
	maxY := math.Min(meanStdevY.mean+float64(sigmaLevel)*meanStdevY.stdev, meta.MaxY) + pointRadius

	// Change the max and min values to ensure the robot marker can be represented in the output image
	var robotMarker spatialmath.Pose
	if ppRM.robotPose != nil {
		robotMarker = *ppRM.robotPose
		minX = math.Min(minX, robotMarker.Point().X-robotMarkerRadius)
		maxX = math.Max(maxX, robotMarker.Point().X+robotMarkerRadius)
		minY = math.Min(minY, robotMarker.Point().Y-robotMarkerRadius)
		maxY = math.Max(maxY, robotMarker.Point().Y+robotMarkerRadius)
	}

	// Calculate the scale factors
	scaleFactor := calculateScaleFactor(maxX-minX, maxY-minY)

	// Add points in the pointcloud to a new image
	im := rimage.NewImage(imageWidth, imageHeight)
	for i := 0; i < im.Width(); i++ {
		for j := 0; j < im.Height(); j++ {
			im.SetXY(i, j, rimage.White)
		}
	}
	cloud.Iterate(0, 0, func(pt r3.Vector, data pointcloud.Data) bool {
		x := int(math.Round((pt.X-meanStdevX.mean)*scaleFactor) + imageWidth/2)
		y := int(math.Round((pt.Y-meanStdevY.mean)*scaleFactor) + imageHeight/2)

		// Adds a point to an image using the value to define the color. If no value is available,
		// the default color of black is used.
		if x >= 0 && x < imageWidth && flipY(y) >= 0 && flipY(y) < imageHeight {
			pointColor := getColorFromProbabilityValue(data)
			im.Circle(image.Point{X: x, Y: flipY(y)}, pointRadius, pointColor)
		}
		return true
	})

	// Add a red robot marker to the image
	if ppRM.robotPose != nil {
		x := int(math.Round((robotMarker.Point().X-meanStdevX.mean)*scaleFactor) + imageWidth/2)
		y := int(math.Round((robotMarker.Point().Y-meanStdevY.mean)*scaleFactor) + imageHeight/2)
		robotMarkerColor := rimage.Red
		if x >= 0 && x < imageWidth && flipY(y) >= 0 && flipY(y) < imageHeight {
			im.Circle(image.Point{X: x, Y: flipY(y)}, robotMarkerRadius, robotMarkerColor)
		}
	}
	return im, nil
}

// RGBDToPointCloud is unimplemented and will produce an error.
func (ppRM *ParallelProjectionOntoXYWithRobotMarker) RGBDToPointCloud(
	img *rimage.Image,
	dm *rimage.DepthMap,
	crop ...image.Rectangle,
) (pointcloud.PointCloud, error) {
	return nil, errors.New("converting an RGB image to Pointcloud is currently unimplemented for this projection")
}

// ImagePointTo3DPoint is unimplemented and will produce an error.
func (ppRM *ParallelProjectionOntoXYWithRobotMarker) ImagePointTo3DPoint(pt image.Point, d rimage.Depth) (r3.Vector, error) {
	return r3.Vector{}, errors.New("converting an image point to a 3D point is currently unimplemented for this projection")
}

// getColorFromProbabilityValue returns an RGB color value based on the probability value
// which is assumed to be in the blue color channel.
// If no data or color is present, 100% probability is assumed.
func getColorFromProbabilityValue(d pointcloud.Data) rimage.Color {
	if d == nil || !d.HasColor() {
		return colorBucket(100)
	}
	_, _, prob := d.RGB255()

	return colorBucket(prob)
}

// NewParallelProjectionOntoXYWithRobotMarker creates a new ParallelProjectionOntoXYWithRobotMarker with the given
// robot pose.
func NewParallelProjectionOntoXYWithRobotMarker(rp *spatialmath.Pose) ParallelProjectionOntoXYWithRobotMarker {
	return ParallelProjectionOntoXYWithRobotMarker{robotPose: rp}
}

// Struct containing the mean and stdev.
type meanStdev struct {
	mean  float64
	stdev float64
}

// Calculates the mean and standard deviation of the X and Y coordinates stored in the point cloud.
func calculatePointCloudMeanAndStdevXY(cloud pointcloud.PointCloud) (meanStdev, meanStdev, error) {
	var X, Y []float64
	var x, y meanStdev

	cloud.Iterate(0, 0, func(pt r3.Vector, data pointcloud.Data) bool {
		X = append(X, pt.X)
		Y = append(Y, pt.Y)
		return true
	})

	meanX, err := safeMath(stats.Mean(X))
	if err != nil {
		return x, y, errors.Wrap(err, "unable to calculate mean of X values on given point cloud")
	}
	x.mean = meanX

	stdevX, err := safeMath(stats.StandardDeviation(X))
	if err != nil {
		return x, y, errors.Wrap(err, "unable to calculate stdev of Y values on given point cloud")
	}
	x.stdev = stdevX

	meanY, err := safeMath(stats.Mean(Y))
	if err != nil {
		return x, y, errors.Wrap(err, "unable to calculate mean of Y values on given point cloud")
	}
	y.mean = meanY

	stdevY, err := safeMath(stats.StandardDeviation(Y))
	if err != nil {
		return x, y, errors.Wrap(err, "unable to calculate stdev of Y values on given point cloud")
	}
	y.stdev = stdevY

	return x, y, nil
}

// Calculates the scaling factor needed to fit the projected pointcloud to the desired image size, cropping it
// if needed based on the mean and standard deviation of the X and Y coordinates.
func calculateScaleFactor(xRange, yRange float64) float64 {
	var scaleFactor float64
	if xRange != 0 || yRange != 0 {
		widthScaleFactor := float64(imageWidth-1) / xRange
		heightScaleFactor := float64(imageHeight-1) / yRange
		scaleFactor = math.Min(widthScaleFactor, heightScaleFactor)
	}
	return scaleFactor
}

// Errors out if overflow has occurred in the given variable or if it is NaN.
func safeMath(v float64, err error) (float64, error) {
	if err != nil {
		return 0, err
	}
	switch {
	case math.IsInf(v, 0):
		return 0, errors.New("overflow detected")
	case math.IsNaN(v):
		return 0, errors.New("NaN detected")
	}
	return v, nil
}

func flipY(y int) int {
	return imageHeight - y
}

// this color map is greyscale. The color map is being used map probability values of a PCD
// into different color buckets provided by the color map.
// generated with: https://grayscale.design/app
// Intended to match the remote-control frontend's slam 2d renderer
// component's color scheme.
var colorMap = []rimage.Color{
	rimage.NewColor(240, 240, 240),
	rimage.NewColor(220, 220, 220),
	rimage.NewColor(200, 200, 200),
	rimage.NewColor(190, 190, 190),
	rimage.NewColor(170, 170, 170),
	rimage.NewColor(150, 150, 150),
	rimage.NewColor(40, 40, 40),
	rimage.NewColor(20, 20, 20),
	rimage.NewColor(10, 10, 10),
	rimage.Black,
}

// Map the color of a pixel to a color bucket value.
func probToColorMapBucket(probability uint8, numBuckets int) int {
	//nolint:mnd
	prob := math.Max(math.Min(100, float64(probability)), 0)
	//nolint:mnd
	return int(math.Floor(float64(numBuckets-1) * prob / 100))
}

// Find the desired color bucket for a given probability. This assumes the probability will be a value from 0 to 100.
func colorBucket(probability uint8) rimage.Color {
	bucket := probToColorMapBucket(probability, len(colorMap))
	return colorMap[bucket]
}

func readNextFileUploadFileChunk(f *bytes.Buffer) (*pbDataSync.FileData, error) {
	byteArr := make([]byte, UploadChunkSize)
	numBytesRead, err := f.Read(byteArr)
	if numBytesRead < UploadChunkSize {
		byteArr = byteArr[:numBytesRead]
	}
	if err != nil {
		return nil, err
	}
	return &pbDataSync.FileData{Data: byteArr}, nil
}

func getNextFileUploadRequest(ctx context.Context, f *bytes.Buffer) (*pbDataSync.FileUploadRequest, error) {
	select {
	case <-ctx.Done():
		return nil, context.Canceled
	default:
		// Get the next file data reading from file, check for an error.
		next, err := readNextFileUploadFileChunk(f)
		if err != nil {
			return nil, err
		}
		// Otherwise, return an UploadRequest and no error.
		return &pbDataSync.FileUploadRequest{
			UploadPacket: &pbDataSync.FileUploadRequest_FileContents{
				FileContents: next,
			},
		}, nil
	}
}

func sendFileUploadRequests(ctx context.Context, stream pbDataSync.DataSyncService_FileUploadClient, f *bytes.Buffer) error {
	// Loop until there is no more content to be read from file.
	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
			// Get the next UploadRequest from the file.
			uploadReq, err := getNextFileUploadRequest(ctx, f)

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
