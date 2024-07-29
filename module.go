// package main implements a slam service that wraps cloudslam
package main

import (
	"context"

	"github.com/viam-modules/cloudslam-wrapper/cloudslam"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/module"
	"go.viam.com/rdk/services/slam"
	"go.viam.com/utils"
)

func main() {
	utils.ContextualMain(mainWithArgs, module.NewLoggerFromArgs("cloudslam_wrapper"))
}

func mainWithArgs(ctx context.Context, args []string, logger logging.Logger) (err error) {
	customModule, err := module.NewModuleFromArgs(ctx, logger)
	if err != nil {
		return err
	}

	err = customModule.AddModelFromRegistry(ctx, slam.API, cloudslam.Model)
	if err != nil {
		return err
	}

	err = customModule.Start(ctx)
	defer customModule.Close(ctx)
	if err != nil {
		return err
	}

	<-ctx.Done()
	return nil
}
