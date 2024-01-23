use async_trait::async_trait;

/// Implement this to give windsock some control over your cloud.
/// Currently the only thing windsock needs is the ability to cleanup resources since resource creation should happen within your own benches.
#[async_trait(?Send)]
pub trait Cloud {
    /// Each bench creates an instance of this to provide its resource requirements
    type CloudResourcesRequired;
    /// This is given to the benches and contains methods or data required to access the instances.
    type CloudResources;

    /// Cleanup all cloud resources created by windsock.
    /// You should destroy not just resources created during this bench run but also resources created in past bench runs that might have missed cleanup due to a panic.
    async fn cleanup_resources(&self);

    /// This is called once at start up before running any benches.
    /// The implementation must return an object containing all the requested cloud resources.
    /// The `required_resources` contains the `CloudResourcesRequired` returned by each bench that will be executed in this run.
    async fn create_resources(
        &self,
        required_resources: Vec<Self::CloudResourcesRequired>,
    ) -> Self::CloudResources;

    /// This is called once at start up before running any benches.
    /// The returned Vec specifies the order in which to run benches.
    fn order_benches(
        &self,
        benches: Vec<BenchInfo<Self::CloudResourcesRequired>>,
    ) -> Vec<BenchInfo<Self::CloudResourcesRequired>> {
        benches
    }

    /// This is called before running each bench.
    /// Use it to destroy or create resources as needed.
    ///
    /// It is recommended to create all resources within create_resources for faster completion time, but it may be desirable in some circumstances to create some of them here.
    /// It is recommended to always destroy resources that will never be used again here.
    async fn adjust_resources(
        &self,
        _benches: &[BenchInfo<Self::CloudResourcesRequired>],
        _bench_index: usize,
        _resources: &mut Self::CloudResources,
    ) {
    }
}

pub struct BenchInfo<CloudResourceRequest> {
    pub name: String,
    pub resources: CloudResourceRequest,
}

/// A dummy cloud instance for when the user isnt using windsock cloud functionality
pub struct NoCloud;
impl NoCloud {
    pub fn new_boxed() -> Box<dyn Cloud<CloudResourcesRequired = (), CloudResources = ()>>
    where
        Self: Sized,
    {
        Box::new(NoCloud)
    }
}

#[async_trait(?Send)]
impl Cloud for NoCloud {
    type CloudResourcesRequired = ();
    type CloudResources = ();
    async fn cleanup_resources(&self) {}
    async fn create_resources(&self, _requests: Vec<()>) {}
}
