package main

import (
	"context"

	"github.com/hashicorp/go-hclog"
	"gitlab.com/gitlab-org/fleeting/fleeting/plugin"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
)

type InstanceGroup struct {
	APIEndpoint string `json:"api_endpoint"`
	Token       string `json:"token"`
	PoolName    string `json:"pool_name"`
}

func (g *InstanceGroup) Init(ctx context.Context, log hclog.Logger, settings provider.Settings) (provider.ProviderInfo, error) {
	// Initialize API client, validate config, return provider metadata
	return provider.ProviderInfo{}, nil
}

// Increase requests more instances to be created. It returns how many
// instances were successfully requested.
func (g *InstanceGroup) Increase(ctx context.Context, delta int) (int, error) {
	// Provision delta new instances
	return delta, nil
}

// Decrease removes the specified instances from the instance group. It
// returns instance IDs of successful requests for removal.
func (g *InstanceGroup) Decrease(ctx context.Context, ids []string) ([]string, error) {
	// Delete or scale down listed instances
	return ids, nil
}

// Update updates instance data from the instance group, passing a function
// to perform instance reconciliation.
func (g *InstanceGroup) Update(ctx context.Context, update func(string, provider.State)) error {
	// Report current instance states back to Runner
	return nil
}

// ConnectInfo returns additional information about an instance,
// useful for creating a connection.
func (g *InstanceGroup) ConnectInfo(ctx context.Context, id string) (provider.ConnectInfo, error) {
	// Return SSH/WinRM connection info for a specific instance
	return provider.ConnectInfo{}, nil
}

// Shutdown performs any cleanup tasks required when the plugin is to shutdown.
func (g *InstanceGroup) Shutdown(ctx context.Context) error {
	return nil
}

// Heartbeat returns an error if there has been an issue detected with a given instance.
// useful for checking if an instance is still alive.
func (g *InstanceGroup) Heartbeat(ctx context.Context, instance string) error {
	return nil
}

func main() {
	plugin.Main(&InstanceGroup{}, plugin.VersionInfo{
		Name:      "fleeting-plugin-myplatform",
		Version:   "dev",
		Revision:  "HEAD",
		Reference: "HEAD",
		BuiltAt:   "now",
	})
}
