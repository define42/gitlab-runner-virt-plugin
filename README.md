# gitlab-runner-virt-plugin

[![codecov](https://codecov.io/gh/define42/gitlab-runner-virt-plugin/graph/badge.svg?token=Q4AR5750VG)](https://codecov.io/gh/define42/gitlab-runner-virt-plugin)

`gitlab-runner-virt-plugin` is a [Fleeting](https://gitlab.com/gitlab-org/fleeting/fleeting) provider for GitLab Runner `docker-autoscaler` setups backed by libvirt.
When configured for single-use instances, it gives GitLab Runner a per-job sandbox VM model similar to standard GitHub-hosted runners.

It does three main things:

1. Clones a Flatcar base image from a libvirt storage pool.
2. Generates an Ignition config from the Runner SSH connector settings.
3. Boots a libvirt VM and reports its SSH address back to Runner.

The generated Ignition config:

- creates or updates the SSH user from `connector_config.username`
- hashes `connector_config.password` into Ignition `passwordHash`
- derives and installs an SSH public key when `connector_config.key` is set
- copies additional PEM-encoded CA root certificates from `plugin_config.ca_certificates_path` when configured
- refreshes Flatcar's system trust store before `docker.service` starts when custom CAs are provided
- enables `docker.service`
- sets `/etc/hostname` to the VM name

For Runner SSH connectivity, use `connector_config.key` or `key_path`.
`connector_config.password` is written into Ignition, but password-based SSH
authentication does not work with the current Runner/Fleeting connector here.

## Requirements

- A working local libvirt daemon, typically reachable as `qemu:///system`
- A Flatcar QEMU image already imported into a libvirt storage pool
  The documented Flatcar image is `https://stable.release.flatcar-linux.net/amd64-usr/current/flatcar_production_qemu_image.img`
  Copy the image onto the libvirt host, typically into `/var/lib/libvirt/images/`, before importing it into the storage pool.
- A libvirt network that hands out DHCP leases
- A `state_dir` path that is readable by the QEMU/libvirt host process

Notes:

- The plugin injects Ignition through QEMU `fw_cfg` using Flatcar's libvirt path `opt/org.flatcar-linux/config`.
- Address discovery defaults to libvirt lease lookup, then falls back to guest-agent and ARP based lookup.
- The implementation assumes the Runner can reach the guest IP that libvirt reports. With the default NAT network this usually means the Runner is running on the same host as libvirt.

## Plugin Config

Required fields:

- `max_size`
- `pool_name`
- `base_volume_name` or `base_volume_path`

Common fields:

```json
{
  "uri": "qemu:///system",
  "pool_name": "default",
  "base_volume_name": "flatcar_production_qemu_image.img",
  "network_name": "default",
  "state_dir": "/var/lib/libvirt/gitlab-runner-virt-plugin",
  "ca_certificates_path": "/etc/gitlab-runner/ca-roots",
  "domain_prefix": "gitlab-runner",
  "max_size": 10,
  "vcpu_count": 2,
  "memory_mib": 4096,
  "disk_size_gib": 40,
  "address_source": "auto"
}
```

Supported fields:

- `uri`: libvirt connection URI, default `qemu:///system`
- `pool_name`: target storage pool for cloned runner disks
- `base_volume_name`: source Flatcar volume inside `pool_name`
- `base_volume_path`: source Flatcar volume path, alternative to `base_volume_name`
- `network_name`: libvirt network name, default `default`
- `state_dir`: where generated Ignition files are written, default `/var/lib/libvirt/gitlab-runner-virt-plugin`
- `ca_certificates_path`: optional host path to a PEM certificate file or directory of PEM certificate files; the plugin writes them into `/etc/ssl/certs` inside the VM and runs `update-ca-certificates`
- `domain_prefix`: prefix for managed domain names, default `gitlab-runner`
- `max_size`: maximum number of VMs Runner may request
- `vcpu_count`: vCPU count per VM, default `2`
- `memory_mib`: memory per VM in MiB, default `4096`
- `disk_size_gib`: optional disk resize target; if larger than the base image, the cloned disk is expanded
- `domain_type`: libvirt domain type, default `kvm`
- `machine_type`: optional machine type passed into the libvirt XML `os/type` element
- `address_source`: `auto`, `lease`, `agent`, or `arp`

## Security Model

For the strongest isolation, use:

- `capacity_per_instance = 1`
- `max_use_count = 1`

That makes Runner create one VM for one job and then discard it. The job container runs against the Docker daemon inside that VM, not the libvirt host's Docker daemon. In practice, this is the GitLab Runner equivalent of the standard GitHub-hosted runner pattern: a fresh VM per job with teardown after the job completes.

This is similar to GitHub-hosted runners, not literally the same service. GitHub manages the hypervisor, image pipeline, and platform hardening for GitHub-hosted runners. With this plugin, you manage the libvirt host, base image contents, patching, network boundaries, and access to secrets.

If you increase `capacity_per_instance` or `max_use_count`, you trade some isolation for better density and faster warm reuse.

## Runner Example

Example full `config.toml` for a GitLab Runner using `docker-autoscaler` with this plugin:

```toml
concurrent = 4
check_interval = 0
connection_max_age = "15m0s"
shutdown_timeout = 0

[session_server]
  session_timeout = 1800

[[runners]]
  name = "myrunner"
  url = "https://gitlab.com"
  id = 52258673
  token = "<private token>"
  executor = "docker-autoscaler"
  shell = "sh"

  [runners.docker]
    image = "alpine:3.20"
    pull_policy = "if-not-present"
    # privileged mode and the Docker socket mount apply inside the guest VM.
    # With capacity_per_instance = 1 and max_use_count = 1, each job gets a
    # fresh sandbox VM that is deleted after the job completes.
    privileged = true
    volumes = ["/var/run/docker.sock:/var/run/docker.sock"]

  [runners.autoscaler]
    plugin = "fleeting-plugin-libvirt"

    capacity_per_instance = 1
    max_use_count = 1
    max_instances = 10
    delete_instances_on_shutdown = true
    log_internal_ip = true
    log_external_ip = true

    [runners.autoscaler.plugin_config]
      uri = "qemu:///system"
      pool_name = "default"
      base_volume_name = "flatcar_production_qemu_image.img"
      network_name = "default"
      state_dir = "/var/lib/libvirt/gitlab-runner-virt-plugin"
      ca_certificates_path = "/etc/gitlab-runner/ca-roots"
      domain_prefix = "gitlab-runner"
      max_size = 10
      vcpu_count = 1
      memory_mib = 512
      disk_size_gib = 17
      address_source = "lease"

    [runners.autoscaler.connector_config]
      os = "linux"
      arch = "amd64"
      protocol = "ssh"
      protocol_port = 22
      username = "core"
      key_path = "/etc/gitlab-runner/libvirt-runner"
      use_static_credentials = true
      timeout = "10m"

    [[runners.autoscaler.policy]]
      idle_count = 3
      idle_time = "20m0s"
      preemptive_mode = true

```

Reuse behavior:

- `max_use_count = 1` means each VM is used for exactly one job and then deleted.
- `max_use_count = 5` means a VM can be reused for up to five jobs before Runner schedules it for removal.
- `capacity_per_instance = 1` keeps the security boundary at one job per VM.
- `capacity_per_instance = 1` plus `max_use_count = 1` is the closest match to the standard GitHub-hosted runner isolation model.
- `concurrent` should typically be `max_instances * capacity_per_instance`.

Use SSH keys for Runner connectivity. Set `key_path`.
The plugin will derive the matching public key and install it via Ignition.
`connector_config.password` does not work for Runner SSH connectivity here.

To create the key referenced by `key_path`, run:

```bash
sudo install -d -m 700 /etc/gitlab-runner
sudo ssh-keygen -t ed25519 -N '' -f /etc/gitlab-runner/libvirt-runner
sudo chmod 600 /etc/gitlab-runner/libvirt-runner
```

## Build

```bash
go build ./...
```

Install the plugin binary with:

```bash
install -m 0755 fleeting-plugin-libvirt_linux_amd64 /usr/local/bin/fleeting-plugin-libvirt
```

## Operational Notes

- Imported Flatcar images should be the official QEMU/libvirt-ready image format.
- `state_dir` must exist on the hypervisor host filesystem because libvirt passes the Ignition file to QEMU by host path.
- `ca_certificates_path` is read on the host running the plugin. It may point to a single PEM file or a directory of PEM files.
- Managed instances are identified by the configured `domain_prefix`.
- `virsh list` can be used to see active VMs.
- The plugin deletes the libvirt domain definition, the cloned storage volume, and the generated Ignition file when Runner scales an instance down.

## References

- Flatcar libvirt provisioning: https://www.flatcar.org/docs/latest/installing/virtualization/libvirt/
- Flatcar authentication examples: https://www.flatcar.org/docs/latest/setup/customization/configuring-flatcar/
- libvirt domain XML and `fw_cfg`: https://libvirt.org/formatdomain.html
- GitHub-hosted runners: https://docs.github.com/en/actions/how-tos/manage-runners/github-hosted-runners/use-github-hosted-runners
- GitHub Actions security hardening: https://docs.github.com/en/actions/security-guides/security-hardening-for-github-actions
