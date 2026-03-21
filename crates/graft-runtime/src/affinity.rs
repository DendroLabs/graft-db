/// Pin the calling thread to a specific CPU core.
///
/// - Linux: hard affinity via `sched_setaffinity`
/// - macOS: advisory affinity via `thread_policy_set`
/// - Other: no-op
///
/// Returns `true` if pinning succeeded, `false` otherwise.
/// Never panics — callers should log and continue on failure.
pub fn pin_to_core(core_id: usize) -> bool {
    #[cfg(target_os = "linux")]
    {
        pin_to_core_linux(core_id)
    }
    #[cfg(target_os = "macos")]
    {
        pin_to_core_macos(core_id)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = core_id;
        tracing::debug!("core pinning not supported on this platform");
        false
    }
}

#[cfg(target_os = "linux")]
fn pin_to_core_linux(core_id: usize) -> bool {
    use std::mem;

    unsafe {
        let mut set: libc::cpu_set_t = mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core_id, &mut set);

        let ret = libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &set);
        if ret == 0 {
            tracing::debug!("pinned thread to core {core_id}");
            true
        } else {
            tracing::warn!(
                "failed to pin thread to core {core_id}: errno {}",
                *libc::__errno_location()
            );
            false
        }
    }
}

#[cfg(target_os = "macos")]
fn pin_to_core_macos(core_id: usize) -> bool {
    // macOS doesn't support hard CPU affinity. thread_policy_set with
    // THREAD_AFFINITY_POLICY is advisory — the kernel uses the tag as a
    // hint to co-locate or separate threads, but doesn't guarantee pinning.
    unsafe {
        let thread = libc::pthread_self();
        // THREAD_AFFINITY_POLICY = 4 on macOS
        const THREAD_AFFINITY_POLICY: u32 = 4;

        #[repr(C)]
        struct ThreadAffinityPolicyData {
            affinity_tag: i32,
        }

        let policy = ThreadAffinityPolicyData {
            affinity_tag: core_id as i32 + 1, // 0 = no affinity, so offset by 1
        };

        // mach_port_t is u32 on macOS
        // pthread_mach_thread_np converts pthread_t → mach thread port
        let mach_thread = pthread_mach_thread_np(thread);

        let kr = thread_policy_set(
            mach_thread,
            THREAD_AFFINITY_POLICY,
            &policy as *const _ as *const i32,
            1, // count = number of i32s in the policy struct
        );

        if kr == 0 {
            tracing::debug!("set advisory affinity tag for core {core_id}");
            true
        } else {
            tracing::warn!("failed to set affinity for core {core_id}: kern_return {kr}");
            false
        }
    }
}

#[cfg(target_os = "macos")]
extern "C" {
    fn thread_policy_set(
        thread: u32, // thread_port_t (mach_port_t)
        flavor: u32, // thread_policy_flavor_t
        policy_info: *const i32,
        count: u32, // mach_msg_type_number_t
    ) -> i32; // kern_return_t

    fn pthread_mach_thread_np(thread: libc::pthread_t) -> u32;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_to_core_zero_does_not_panic() {
        // Should succeed or gracefully return false, never panic
        let _ = pin_to_core(0);
    }

    #[test]
    fn pin_to_invalid_core_does_not_panic() {
        // A very large core_id should fail gracefully
        let _ = pin_to_core(99999);
    }
}
