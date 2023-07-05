use async_trait::async_trait;

/// Implement this to give windsock some control over your cloud.
/// Currently the only thing windsock needs is the ability to cleanup resources since resource creation should happen within your own benches.
#[async_trait]
pub trait Cloud {
    /// Cleanup all cloud resources created by windsock.
    /// You should destroy not just resources created during this bench run but also resources created in past bench runs that might have missed cleanup due to a panic.
    async fn cleanup_resources(&self);

    /// Create a dummy instance for when the user isnt using windsock cloud functionality
    fn none() -> Box<dyn Cloud>
    where
        Self: Sized,
    {
        Box::new(NoCloud)
    }
}

pub(crate) struct NoCloud;

#[async_trait]
impl Cloud for NoCloud {
    async fn cleanup_resources(&self) {}
}
