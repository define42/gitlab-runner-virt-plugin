package main

import (
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"gitlab.com/gitlab-org/fleeting/fleeting/plugin"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
)

type InstanceGroup struct {
	URI            string `json:"uri"`
	PoolName       string `json:"pool_name"`
	BaseVolumeName string `json:"base_volume_name,omitempty"`
	BaseVolumePath string `json:"base_volume_path,omitempty"`
	NetworkName    string `json:"network_name,omitempty"`
	DomainPrefix   string `json:"domain_prefix,omitempty"`
	StateDir       string `json:"state_dir,omitempty"`
	MaxSize        int    `json:"max_size"`
	VCPUCount      uint   `json:"vcpu_count,omitempty"`
	MemoryMiB      uint   `json:"memory_mib,omitempty"`
	DiskSizeGiB    uint   `json:"disk_size_gib,omitempty"`
	DomainType     string `json:"domain_type,omitempty"`
	MachineType    string `json:"machine_type,omitempty"`
	AddressSource  string `json:"address_source,omitempty"`

	mu             sync.Mutex           `json:"-"`
	settings       provider.Settings    `json:"-"`
	logger         hclog.Logger         `json:"-"`
	deleting       map[string]time.Time `json:"-"`
	passwordHash   string               `json:"-"`
	authorizedKeys []string             `json:"-"`
}

func main() {
	plugin.Main(&InstanceGroup{}, plugin.VersionInfo{
		Name:      "fleeting-plugin-libvirt",
		Version:   "dev",
		Revision:  "HEAD",
		Reference: "HEAD",
		BuiltAt:   "now",
	})
}
