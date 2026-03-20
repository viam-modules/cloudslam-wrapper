package cloudslam

import (
	"github.com/golang/geo/r3"
	"go.viam.com/rdk/pointcloud"
)

// font5x7 defines a 5×7 dot-matrix bitmap for each character used in status text.
// Each entry is 7 bytes (one per row, top to bottom). Within each byte,
// bit 4 is the leftmost column and bit 0 is the rightmost.
var font5x7 = map[byte][7]byte{
	' ': {},
	'A': {0b01110, 0b10001, 0b10001, 0b11111, 0b10001, 0b10001, 0b10001},
	'E': {0b11111, 0b10000, 0b11110, 0b10000, 0b10000, 0b10000, 0b11111},
	'F': {0b11111, 0b10000, 0b11110, 0b10000, 0b10000, 0b10000, 0b10000},
	'G': {0b01110, 0b10001, 0b10000, 0b10110, 0b10001, 0b10001, 0b01110},
	'I': {0b11111, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0b11111},
	'D': {0b11110, 0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b11110},
	'L': {0b10000, 0b10000, 0b10000, 0b10000, 0b10000, 0b10000, 0b11111},
	'N': {0b10001, 0b11001, 0b10101, 0b10011, 0b10001, 0b10001, 0b10001},
	'O': {0b01110, 0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b01110},
	'R': {0b11110, 0b10001, 0b10001, 0b11110, 0b10100, 0b10010, 0b10001},
	'S': {0b01110, 0b10001, 0b10000, 0b01110, 0b00001, 0b10001, 0b01110},
	'T': {0b11111, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100},
	'W': {0b10001, 0b10001, 0b10001, 0b10101, 0b10101, 0b11011, 0b10001},
}

// addTextToPCD renders a string into a point cloud using a 5×7 dot-matrix font.
// (x0, y0) is the top-left corner of the first character in mm.
// pixelSize controls the spacing between dots in mm.
func addTextToPCD(pc pointcloud.PointCloud, text string, x0, y0, pixelSize float64) error {
	for i, ch := range []byte(text) {
		bitmap, ok := font5x7[ch]
		if !ok {
			continue
		}
		charX := x0 + float64(i)*6*pixelSize
		for row := range 7 {
			for col := range 5 {
				if bitmap[row]&(1<<(4-col)) != 0 {
					px := charX + float64(col)*pixelSize
					py := y0 - float64(row)*pixelSize
					if err := pc.Set(r3.Vector{X: px, Y: py, Z: 0}, pointcloud.NewBasicData()); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
