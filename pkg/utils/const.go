package utils

const (
	ResourceName = "shared-gpu/gpu-mem"
	CountName    = "shared-gpu/gpu-count"

	EnvNVGPU              = "NVIDIA_VISIBLE_DEVICES"
	EnvResourceIndex      = "SHARED_GPU_MEM_IDX"
	EnvResourceByPod      = "SHARED_GPU_MEM_POD"
	EnvResourceByDev      = "SHARED_GPU_MEM_DEV"
	EnvAssignedFlag       = "SHARED_GPU_MEM_ASSIGNED"
	EnvResourceAssumeTime = "SHARED_GPU_MEM_ASSUME_TIME"
)
