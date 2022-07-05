use std::{any::Any, fmt};

pub trait SqlValue: Any + BoxClone + fmt::Debug {}

pub trait BoxClone {
    fn clone_box(&self) -> Box<dyn SqlValue>;
}

impl<T: 'static + Clone + fmt::Debug> BoxClone for T {
    fn clone_box(&self) -> Box<dyn SqlValue> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn SqlValue> {
    fn clone(&self) -> Self {
        // break the recursion with `.as_ref()`
        self.as_ref().clone_box()
    }
}

impl<T: Any + BoxClone + fmt::Debug> SqlValue for T {}
