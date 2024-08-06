# [`cloudslam wrapper` module](https://github.com/viam-modules/cloudslam-wrapper)

This [module](https://docs.viam.com/registry/#modular-resources) implements the [`rdk:service:slam` API](https://docs.viam.com/services/slam/) in a cloudslam-wrapper model.
With this model, you can start, view, and stop cloudslam mapping sessions by using the DoCommands defined within the module, as well as upload a locally built SLAM map to the registry.
Currently, only [Cartographer](https://docs.viam.com/services/slam/cartographer/) is supported for cloudslam, but all SLAM services can use the local map upload feature.

## Configure your cloudslam-wrapper

Navigate to the [**CONFIGURE** tab](https://docs.viam.com/configure/) of your [machine](https://docs.viam.com/fleet/machines/) in [the Viam app](https://app.viam.com/).
[Add <slam / cloudslam-wrapper> to your machine](https://docs.viam.com/configure/#services).

On the new service panel, copy and paste the following attribute template into your SLAM’s attributes field:

```json
{
   "slam_service": "<slam-service-name>",
   "api_key": "<location-api-key>",
   "api_key_id": "<location-api-key-id>",
   "organization_id": "<organization_id>",
   "location_id": "<location_id>",
   "robot_id": "<robot_id>",
}
```

### Attributes

The following attributes are available for `<INSERT MODEL TRIPLET>` <INSERT API NAME>s:

| Name    | Type   | Required?    | Description |
| ------- | ------ | ------------ | ----------- |
| `slam_service` | string | **Required** | Name of the SLAM Service on the robot to use with cloudslam        |
| `api_key` | string | **Required**     | [location owner API key](https://docs.viam.com/cloud/rbac/#add-an-api-key) needed to use cloudslam apis        |
| `api_key_id` | string | **Required**     | location owner API key id        |
| `organization_id` | string | **Required**     | id string for your [organization](https://docs.viam.com/cloud/organizations/)        |
| `location_id` | string | **Required**     | id string for your [location](https://docs.viam.com/cloud/locations/)        |
| `machine_id` | string | **Required**     | id string for your [machine](https://docs.viam.com/appendix/apis/fleet/#find-machine-id)        |
| `machine_part_id` | string | Optional     | optional id string for the [machine part](https://docs.viam.com/appendix/apis/fleet/#find-machine-id). only used when using local package creation        |
| `viam_version` | string | Optional     | optional string to identify which version of viam-server to use with cloudslam. Defaults to `stable`        |
| `slam_version` | string | Optional     | optional string to identify which version of cartographer to use with cloudslam. Defaults to `stable`         |
| `camera_freq_hz` | float | Optional     | set the expected capture frequency for your camera/lidar components. Defaults to `5`        |
| `movement_sensor_freq_hz` | float | Optional     | set the expected capture frequency for your movement sensor components. Defaults to `20`        |

### Example configuration

```json
{
  "slam_service": "my-actual-slam-service",
   "api_key": "location-api-key",
   "api_key_id": "location-api-key-id",
   "organization_id": "organization_id",
   "location_id": "location_id",
   "machine_id": "machine_id",
   "machine_part_id": "machine_part_id", 
   "camera_freq_hz": 5.0,
   "movement_sensor_freq_hz": 20.0, 
   "slam_version": "stable", 
   "viam_version": "stable", 
}
```

### Next steps - Using the cloudslam service
To interact with a cloudslam mapping session, go to the `DoCommand` on the [Control tab](https://docs.viam.com/cloud/machines/#control) and select your cloudslam wrapper service from the dropdown. From here, you can use the following commands
- {`"start": "<MAP_NAME>"`} will start a cloudslam mapping session using the configured SLAM service. If the request is successful the current map will appear in the cloudslam-wrapper's service card after ~1 minute
- {`"stop": ""`} will stop an active cloudslam mapping session if one is running. The completed map can be found on the SLAM library tab of the robots page
- {`"save-local-map": "<MAP_NAME>"`} will grab the current map from the configured SLAM service and upload it to your location, in the SLAM library tab of the robots page
