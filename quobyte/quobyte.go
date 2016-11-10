package quobyte

import (
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/Sirupsen/logrus"
	quobyte_api "github.com/quobyte/api"
	"github.com/rancher/convoy/util"

	. "github.com/rancher/convoy/convoydriver"
)

const (
	DRIVER_NAME        = "quobyte"
	DRIVER_CONFIG_FILE = "quobyte.cfg"

	VOLUME_CFG_PREFIX = "volume_"
	CFG_PREFIX        = DRIVER_NAME + "_"
	CFG_POSTFIX       = ".json"

	MOUNTS_DIR = "mounts"

	QUOBYTE_API_URL      = "quobyte.apiurl"
	QUOBYTE_API_USER     = "quobyte.apiuser"
	QUOBYTE_API_PASSWORD = "quobyte.apipassword"

	QUOBYTE_REGISTRIES            = "quobyte.registries"
	QUOBYTE_DEFAULT_USER          = "quobyte.defaultuser"
	QUOBYTE_DEFAULT_GROUP         = "quobyte.defaultgroup"
	QUOBYTE_DEFAULT_VOLUME_CONFIG = "quobyte.defaultvolumeconfig"
	//TODO -> Tenant?
)

var (
	log = logrus.WithFields(logrus.Fields{"pkg": "quobyte"})
)

type Driver struct {
	mutex  *sync.RWMutex
	client *quobyte_api.QuobyteClient
	Device
}

func init() {
	Register(DRIVER_NAME, Init)
}

func (d *Driver) Name() string {
	return DRIVER_NAME
}

type Device struct {
	Root              string
	Registries        string
	User              string
	Group             string
	VolumeConfig      string
	DefaultVolumeSize int64
}

func (dev *Device) ConfigFile() (string, error) {
	if dev.Root == "" {
		return "", fmt.Errorf("BUG: Invalid empty device config path")
	}
	return filepath.Join(dev.Root, DRIVER_CONFIG_FILE), nil
}

type QuobyteVolume struct {
	Name       string
	ID         string
	MountPoint string
	configPath string
	User       string
	Group      string
	Device     string
	Config     string
}

func (v *QuobyteVolume) ConfigFile() (string, error) {
	if v.Name == "" {
		return "", errors.New("empty volume")
	}
	if v.configPath == "" {
		return "", errors.New("empty config path")
	}

	return filepath.Join(v.configPath, CFG_PREFIX+VOLUME_CFG_PREFIX+v.Name+CFG_POSTFIX), nil
}

func (v *QuobyteVolume) GetMountOpts() []string {
	return []string{"-t", "quobyte"}
}

func (v *QuobyteVolume) GetDevice() (string, error) {
	return v.Device, nil
}

func (v *QuobyteVolume) GenerateDefaultMountPoint() string {
	return filepath.Join(v.configPath, MOUNTS_DIR, v.Name)
}

func (device *Device) listVolumeNames() ([]string, error) {
	return util.ListConfigIDs(device.Root, CFG_PREFIX+VOLUME_CFG_PREFIX, CFG_POSTFIX)
}

func Init(root string, config map[string]string) (ConvoyDriver, error) {
	dev := &Device{
		Root: root,
	}
	exists, err := util.ObjectExists(dev)
	if err != nil {
		return nil, err
	}
	if exists {
		if err := util.ObjectLoad(dev); err != nil {
			return nil, err
		}
	} else {
		if err := util.MkdirIfNotExists(root); err != nil {
			return nil, err
		}

		for _, req := range []string{QUOBYTE_API_URL, QUOBYTE_REGISTRIES} {
			if config[req] == "" {
				return nil, fmt.Errorf("Missing required parameter: %v", config[req])
			}
		}

		registryList := config[QUOBYTE_REGISTRIES]
		for _, registry := range strings.Split(registryList, ",") {
			host, _, err := net.SplitHostPort(registry)
			if err != nil {
				return nil, fmt.Errorf("Invalid or unsolvable address: %v", err)
			}
			if !util.ValidNetworkAddr(host) {
				return nil, fmt.Errorf("Invalid or unsolvable address: %v", host)
			}
		}

		if _, exists := config[QUOBYTE_API_USER]; !exists {
			config[QUOBYTE_API_USER] = "admin"
		}

		if _, exists := config[QUOBYTE_API_PASSWORD]; !exists {
			config[QUOBYTE_API_PASSWORD] = "quobyte"
		}

		if _, exists := config[QUOBYTE_DEFAULT_USER]; !exists {
			config[QUOBYTE_DEFAULT_USER] = "root"
		}

		if _, exists := config[QUOBYTE_DEFAULT_GROUP]; !exists {
			config[QUOBYTE_DEFAULT_GROUP] = "nfsnobody"
		}

		if _, exists := config[QUOBYTE_DEFAULT_VOLUME_CONFIG]; !exists {
			config[QUOBYTE_DEFAULT_VOLUME_CONFIG] = "BASE"
		}

		dev = &Device{
			Root:         root,
			Registries:   registryList,
			User:         config[QUOBYTE_DEFAULT_USER],
			Group:        config[QUOBYTE_DEFAULT_GROUP],
			VolumeConfig: config[QUOBYTE_DEFAULT_VOLUME_CONFIG],
		}
	}

	driver := &Driver{
		mutex:  &sync.RWMutex{},
		Device: *dev,
		client: quobyte_api.NewQuobyteClient(config[QUOBYTE_API_URL], config[QUOBYTE_API_USER], config[QUOBYTE_API_PASSWORD]),
	}

	if err := driver.remountVolumes(); err != nil {
		return nil, err
	}
	return driver, nil
}

func (d *Driver) Info() (map[string]string, error) {
	return map[string]string{
		"Root":         d.Root,
		"Registries":   fmt.Sprintf("%v", d.Registries),
		"User":         d.User,
		"VolumeConfig": d.VolumeConfig,
	}, nil
}

func (d *Driver) VolumeOps() (VolumeOperations, error) {
	return d, nil
}

func (d *Driver) blankVolume(name string) *QuobyteVolume {
	return &QuobyteVolume{
		configPath: d.Root,
		Name:       name,
	}
}

func (d *Driver) remountVolumes() error {
	volumes, err := util.ListConfigIDs(d.Root, CFG_PREFIX+VOLUME_CFG_PREFIX, CFG_POSTFIX)
	if err != nil {
		return err
	}

	for _, id := range volumes {
		vol := d.blankVolume(id)
		if err := util.ObjectLoad(vol); err != nil {
			return err
		}
		if vol.MountPoint == "" {
			continue
		}

		req := Request{
			Name:    id,
			Options: map[string]string{},
		}
		if _, err := d.MountVolume(req); err != nil {
			return err
		}
	}

	return nil
}

func (d *Driver) getSize(opts map[string]string, defaultVolumeSize int64) (int64, error) {
	//TODO Quobyte Volume has no size
	return 0, nil
}

func (d *Driver) CreateVolume(req Request) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	id := req.Name
	vol := d.blankVolume(id)
	exists, err := util.ObjectExists(vol)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("volume %s already exists", id)
	}

	volume_uuid, err := d.client.CreateVolume(&quobyte_api.CreateVolumeRequest{
		Name:              id,
		RootUserID:        d.User,
		RootGroupID:       d.Group,
		ConfigurationName: d.VolumeConfig,
	})

	if err != nil {
		return err
	}

	vol.Name = id
	vol.ID = volume_uuid
	vol.User = d.User
	vol.Group = d.Group
	vol.Config = d.VolumeConfig
	vol.Device = vol.Name

	return util.ObjectSave(vol)
}

func (d *Driver) DeleteVolume(req Request) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	id := req.Name
	opts := req.Options

	volume := d.blankVolume(id)
	if err := util.ObjectLoad(volume); err != nil {
		return err
	}

	if volume.MountPoint != "" {
		return fmt.Errorf("Cannot delete volume %v. It is still mounted", id)
	}

	referenceOnly, _ := strconv.ParseBool(opts[OPT_REFERENCE_ONLY])
	if !referenceOnly {
		log.Debugf("Cleaning up volume %v", id)
		if err := d.client.DeleteVolume(volume.ID); err != nil {
			return err
		}
	}
	return util.ObjectDelete(volume)
}

func (d *Driver) MountVolume(req Request) (string, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	id := req.Name
	opts := req.Options

	vol := d.blankVolume(id)
	if err := util.ObjectLoad(vol); err != nil {
		return "", err
	}

	mountPoint, err := util.VolumeMount(vol, opts[OPT_MOUNT_POINT], false)
	if err != nil {
		return "", err
	}

	if err := util.ObjectSave(vol); err != nil {
		return "", err
	}

	return mountPoint, nil
}

func (d *Driver) UmountVolume(req Request) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	vol := d.blankVolume(req.Name)
	if err := util.ObjectLoad(vol); err != nil {
		return err
	}

	if err := util.VolumeUmount(vol); err != nil {
		return err
	}

	return util.ObjectSave(vol)
}

func (d *Driver) MountPoint(req Request) (string, error) {
	vol := d.blankVolume(req.Name)
	if err := util.ObjectLoad(vol); err != nil {
		return "", err
	}

	return vol.MountPoint, nil
}

func (d *Driver) GetVolumeInfo(name string) (map[string]string, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	vol := d.blankVolume(name)
	if err := util.ObjectLoad(vol); err != nil {
		return nil, err
	}

	// TODO implement in api
	/* quobyteVol, err := d.client.VolumeINfo()
	if err != nil {
		return nil, err
	}*/

	//TODO User, Group, Config
	info := map[string]string{
		"MountPoint":    vol.MountPoint,
		"ID":            vol.ID,
		OPT_VOLUME_NAME: name,
	}
	return info, nil
}

func (d *Driver) ListVolume(opts map[string]string) (map[string]map[string]string, error) {
	volumes, err := util.ListConfigIDs(d.Root, CFG_PREFIX+VOLUME_CFG_PREFIX, CFG_POSTFIX)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]map[string]string)
	for _, id := range volumes {
		ret[id], err = d.GetVolumeInfo(id)
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func (d *Driver) SnapshotOps() (SnapshotOperations, error) {
	return nil, fmt.Errorf("Doesn't support snapshot operations")
}

func (d *Driver) BackupOps() (BackupOperations, error) {
	return nil, fmt.Errorf("Doesn't support backup operations")
}
