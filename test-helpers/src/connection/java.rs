use anyhow::Result;
use j4rs::{Instance, InvocationArg, Jvm as J4rsJvm, JvmBuilder, MavenArtifact};
use serde::de::DeserializeOwned;
use std::{any::Any, rc::Rc};

/// Runs a JVM in the background and provides methods for constructing java [`Values`].
#[derive(Clone)]
pub(crate) struct Jvm(Rc<J4rsJvm>);

impl Jvm {
    /// Construct a JVM, downloads the list of deps from the maven repository.
    /// Example dep string: "org.slf4j:slf4j-api:1.7.36"
    pub(crate) fn new(deps: &[&str]) -> Self {
        let jvm = Rc::new(JvmBuilder::new().build().unwrap());

        for dep in deps {
            jvm.deploy_artifact(&MavenArtifact::from(*dep)).unwrap();
        }
        Self(jvm)
    }

    /// Call the java constructor with the provided name and args
    pub(crate) fn construct(&self, name: &str, args: Vec<Value>) -> Value {
        self.construct_fallible(name, args).unwrap()
    }

    /// Call the java constructor with the provided name and args
    pub(crate) fn construct_fallible(&self, name: &str, args: Vec<Value>) -> Result<Value> {
        let args: Vec<InvocationArg> = args.into_iter().map(|x| x.instance.into()).collect();
        Ok(Value {
            instance: self.0.create_instance(name, &args)?,
            jvm: self.0.clone(),
        })
    }

    /// Create a java `java.lang.String`.
    pub(crate) fn new_string(&self, string: &str) -> Value {
        Value {
            instance: self
                .0
                .create_instance(
                    "java.lang.String",
                    &[&InvocationArg::try_from(string).unwrap()],
                )
                .unwrap(),
            jvm: self.0.clone(),
        }
    }

    /// Create a java `long`.
    pub(crate) fn new_long(&self, value: i64) -> Value {
        self.new_primitive(
            "java.lang.Long",
            "longValue",
            InvocationArg::try_from(value).unwrap(),
        )
    }

    /// Create a java `int`.
    pub(crate) fn new_int(&self, value: i32) -> Value {
        self.new_primitive(
            "java.lang.Integer",
            "intValue",
            InvocationArg::try_from(value).unwrap(),
        )
    }

    /// Create a java `short`.
    pub(crate) fn new_short(&self, value: i16) -> Value {
        self.new_primitive(
            "java.lang.Short",
            "shortValue",
            InvocationArg::try_from(value).unwrap(),
        )
    }

    fn new_primitive(&self, class_name: &str, convert_method: &str, value: InvocationArg) -> Value {
        Value {
            instance: self
                .0
                .invoke(
                    &self
                        .0
                        .create_instance(
                            // e.g. "java.lang.Long",
                            class_name,
                            &[&value.into_primitive().unwrap()],
                        )
                        .unwrap(),
                    // e.g. "longValue"
                    convert_method,
                    InvocationArg::empty(),
                )
                .unwrap(),
            jvm: self.0.clone(),
        }
    }

    /// Construct a java list containing the provided elements
    pub(crate) fn new_list(&self, element_type: &str, elements: Vec<Value>) -> Value {
        let args: Vec<InvocationArg> = elements.into_iter().map(|x| x.instance.into()).collect();
        Value {
            // TODO: Discuss with upstream why this was deprecated,
            // `Jvm::java_list` is very difficult to use due to Result in input.
            #[allow(deprecated)]
            instance: self.0.create_java_list(element_type, &args).unwrap(),
            jvm: self.0.clone(),
        }
    }

    /// Construct a java map containing the provided key value pairs
    pub(crate) fn new_map(&self, key_values: Vec<(Value, Value)>) -> Value {
        let map = self.construct("java.util.HashMap", vec![]);
        for (k, v) in key_values {
            map.call("put", vec![k, v]);
        }
        map
    }

    /// Call the specified static method on the specified class with the specified arguments.
    pub(crate) fn call_static(
        &self,
        class_name: &str,
        method_name: &str,
        args: Vec<Value>,
    ) -> Value {
        self.call_static_fallible(class_name, method_name, args)
            .unwrap()
    }

    /// Call the specified static method on the specified class with the specified arguments.
    pub(crate) fn call_static_fallible(
        &self,
        class_name: &str,
        method_name: &str,
        args: Vec<Value>,
    ) -> Result<Value> {
        let args: Vec<InvocationArg> = args.into_iter().map(|x| x.instance.into()).collect();
        Ok(Value {
            instance: self.0.invoke_static(class_name, method_name, &args)?,
            jvm: self.0.clone(),
        })
    }

    /// Returns the specified class
    pub(crate) fn class(&self, class_name: &str) -> Value {
        self.class_fallible(class_name).unwrap()
    }

    fn class_fallible(&self, class_name: &str) -> Result<Value> {
        Ok(Value {
            instance: self.0.static_class(class_name)?,
            jvm: self.0.clone(),
        })
    }
}

/// An instance of a class or primitive in the jvm
pub(crate) struct Value {
    instance: Instance,
    jvm: Rc<J4rsJvm>,
}

impl Value {
    /// Call the specified method on this value
    pub(crate) fn call(&self, method_name: &str, args: Vec<Value>) -> Self {
        self.call_fallible(method_name, args).unwrap()
    }

    fn call_fallible(&self, name: &str, args: Vec<Value>) -> Result<Self> {
        let args: Vec<InvocationArg> = args.into_iter().map(|x| x.instance.into()).collect();
        let instance = self.jvm.invoke(&self.instance, name, &args)?;
        Ok(Self {
            instance,
            jvm: self.jvm.clone(),
        })
    }

    /// Call the specified method on this value
    pub(crate) async fn call_async(&self, name: &str, args: Vec<Value>) -> Self {
        self.call_async_fallible(name, args).await.unwrap()
    }

    /// Call the specified method on this value
    pub(crate) async fn call_async_fallible(&self, name: &str, args: Vec<Value>) -> Result<Self> {
        let args: Vec<InvocationArg> = args.into_iter().map(|x| x.instance.into()).collect();
        let instance = self.jvm.invoke_async(&self.instance, name, &args).await?;
        Ok(Self {
            instance,
            jvm: self.jvm.clone(),
        })
    }

    /// Cast this value to another type
    pub(crate) fn cast(&self, name: &str) -> Self {
        self.cast_fallible(name).unwrap()
    }

    fn cast_fallible(&self, name: &str) -> Result<Self> {
        let instance = self.jvm.cast(&self.instance, name)?;
        Ok(Self {
            instance,
            jvm: self.jvm.clone(),
        })
    }

    /// Return the specified field of this value
    pub(crate) fn field(&self, field_name: &str) -> Self {
        self.field_fallible(field_name).unwrap()
    }

    fn field_fallible(&self, field_name: &str) -> Result<Self> {
        let instance = self.jvm.field(&self.instance, field_name)?;
        Ok(Self {
            instance,
            jvm: self.jvm.clone(),
        })
    }

    /// Convert this java value into a native rust type
    pub(crate) fn into_rust<T>(self) -> T
    where
        T: DeserializeOwned + Any,
    {
        self.into_rust_fallible().unwrap()
    }

    fn into_rust_fallible<T>(self) -> Result<T>
    where
        T: DeserializeOwned + Any,
    {
        Ok(self.jvm.to_rust(self.instance)?)
    }
}

impl IntoIterator for Value {
    type Item = Value;

    type IntoIter = JavaIterator;

    fn into_iter(self) -> Self::IntoIter {
        JavaIterator::new(self)
    }
}

/// Implements iteration logic for a java.util.Iterator
/// https://docs.oracle.com/javase/8/docs/api/java/util/Iterator.html
pub(crate) struct JavaIterator(Value);

impl JavaIterator {
    fn new(iterator_instance: Value) -> Self {
        JavaIterator(iterator_instance)
    }
}

impl Iterator for JavaIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Value> {
        let has_next: bool = self.0.call("hasNext", vec![]).into_rust();
        if has_next {
            Some(self.0.call("next", vec![]))
        } else {
            None
        }
    }
}
