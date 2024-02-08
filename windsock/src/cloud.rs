use std::path::Path;

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
    async fn cleanup_resources(&mut self);

    /// This is called once at start up before running any benches.
    /// The implementation must return an object containing all the requested cloud resources.
    /// The `required_resources` contains the `CloudResourcesRequired` returned by each bench that will be executed in this run.
    ///
    /// benches_will_run:
    /// * true  - the benches will be run, ensure they have everything they need to complete succesfully.
    /// * false - the benches will not be run, due to `--store-cloud-resources-file`, you can skip uploading anything that will be reuploaded when restoring the resources
    async fn create_resources(
        &mut self,
        required_resources: Vec<Self::CloudResourcesRequired>,
        benches_will_run: bool,
    ) -> Self::CloudResources;

    /// Construct a file at the provided path that will allow restoring the passed resources
    ///
    /// It is gauranteed this will be called after all the benches have completed.
    async fn store_resources_file(&mut self, path: &Path, resources: Self::CloudResources);

    /// Restore the resources from the data in the passed file.
    /// It is the same file path that was passed to [`Cloud::store_resources_file`]
    ///
    /// The implementation should panic when the loaded messages cannot meet the requirements of the passed `required_sources`.
    /// This is done rather than loading the required resources from disk as this case usually represents a user error.
    /// Loading from disk is used for more consistent results across benches but the user cannot hope to get consistent results while changing the benches that will be run.
    /// They are better off recreating the resources from scratch in this case.
    async fn load_resources_file(
        &mut self,
        path: &Path,
        required_resources: Vec<Self::CloudResourcesRequired>,
    ) -> Self::CloudResources;

    /// This is called once at start up before running any benches.
    /// The returned Vec specifies the order in which to run benches.
    fn order_benches(
        &mut self,
        benches: Vec<BenchInfo<Self::CloudResourcesRequired>>,
    ) -> Vec<BenchInfo<Self::CloudResourcesRequired>> {
        benches
    }

    /// This is called before running each bench.
    /// Use it to destroy or create resources as needed.
    /// However, this method will not be called when `--save-resources-file` or `--load-resources-file` is set.
    ///
    /// It is recommended to create all resources within create_resources for faster completion time, but it may be desirable in some circumstances to create some of them here.
    /// It is recommended to always destroy resources that will never be used again here.
    async fn adjust_resources(
        &mut self,
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
    async fn cleanup_resources(&mut self) {}
    async fn create_resources(&mut self, _requests: Vec<()>, _benches_will_run: bool) {}
    async fn store_resources_file(&mut self, _path: &Path, _resources: ()) {}
    async fn load_resources_file(
        &mut self,
        _path: &Path,
        _required_resources: Vec<Self::CloudResourcesRequired>,
    ) {
    }
}
