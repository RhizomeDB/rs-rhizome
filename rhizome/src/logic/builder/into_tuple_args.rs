pub trait IntoTupleArgs<I, O> {
    type Output;

    fn into_vec(&self) -> Vec<I>;
    fn into_tuple_args(&self, bindings: &[O]) -> Self::Output;
}

impl<I, O> IntoTupleArgs<I, O> for () {
    type Output = ();

    fn into_vec(&self) -> Vec<I> {
        vec![]
    }

    fn into_tuple_args(&self, bindings: &[O]) -> Self::Output {
        assert!(bindings.len() == 0);

        ()
    }
}

impl<I, O> IntoTupleArgs<I, O> for (I,)
where
    I: Clone,
    O: Clone,
{
    type Output = (O,);

    fn into_vec(&self) -> Vec<I> {
        vec![self.0.clone()]
    }

    fn into_tuple_args(&self, bindings: &[O]) -> Self::Output {
        assert!(bindings.len() == 1);

        (bindings[0].clone(),)
    }
}

impl<I, O> IntoTupleArgs<I, O> for (I, I)
where
    I: Clone,
    O: Clone,
{
    type Output = (O, O);

    fn into_vec(&self) -> Vec<I> {
        vec![self.0.clone(), self.1.clone()]
    }

    fn into_tuple_args(&self, bindings: &[O]) -> Self::Output {
        assert!(bindings.len() == 2);

        (bindings[0].clone(), bindings[1].clone())
    }
}

impl<I, O> IntoTupleArgs<I, O> for (I, I, I)
where
    I: Clone,
    O: Clone,
{
    type Output = (O, O, O);

    fn into_vec(&self) -> Vec<I> {
        vec![self.0.clone(), self.1.clone(), self.2.clone()]
    }

    fn into_tuple_args(&self, bindings: &[O]) -> Self::Output {
        assert!(bindings.len() == 3);

        (
            bindings[0].clone(),
            bindings[1].clone(),
            bindings[2].clone(),
        )
    }
}

impl<I, O> IntoTupleArgs<I, O> for (I, I, I, I)
where
    I: Clone,
    O: Clone,
{
    type Output = (O, O, O, O);

    fn into_vec(&self) -> Vec<I> {
        vec![
            self.0.clone(),
            self.1.clone(),
            self.2.clone(),
            self.3.clone(),
        ]
    }

    fn into_tuple_args(&self, bindings: &[O]) -> Self::Output {
        assert!(bindings.len() == 4);

        (
            bindings[0].clone(),
            bindings[1].clone(),
            bindings[2].clone(),
            bindings[3].clone(),
        )
    }
}

impl<I, O> IntoTupleArgs<I, O> for (I, I, I, I, I)
where
    I: Clone,
    O: Clone,
{
    type Output = (O, O, O, O, O);

    fn into_vec(&self) -> Vec<I> {
        vec![
            self.0.clone(),
            self.1.clone(),
            self.2.clone(),
            self.3.clone(),
            self.4.clone(),
        ]
    }

    fn into_tuple_args(&self, bindings: &[O]) -> Self::Output {
        assert!(bindings.len() == 5);

        (
            bindings[0].clone(),
            bindings[1].clone(),
            bindings[2].clone(),
            bindings[3].clone(),
            bindings[4].clone(),
        )
    }
}
