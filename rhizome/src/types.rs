use \
impl FromType<i32> for Type {
    fn from_type() -> Self {
        Self::S32
    }
}

impl FromType<u32> for Type {
    fn from_type() -> Self {
        Self::U32
    }
}

impl FromType<i64> for Type {
    fn from_type() -> Self {
        Self::S64
    }
}

impl FromType<u64> for Type {
    fn from_type() -> Self {
        Self::U64
    }
}

impl FromType<char> for Type {
    fn from_type() -> Self {
        Self::Char
    }
}

impl FromType<&str> for Type {
    fn from_type() -> Self {
        Self::String
    }
}

impl FromType<Cid> for Type {
    fn from_type() -> Self {
        Self::Cid
    }
}

impl Display for ColType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColType::Any => f.write_str("any"),
            ColType::Type(t) => Display::fmt(t, f),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Type {
    Bool,
    S8,
    U8,
    S16,
    U16,
    S32,
    U32,
    S64,
    U64,
    Char,
    String,
    Cid,
    Js,
}

impl Type {
    pub fn new<T>() -> Self
    where
        Self: FromType<T>,
    {
        FromType::<T>::from_type()
    }

    pub fn check(&self, value: &Val) -> Result<()> {
        let other = &value.type_of();

        self.unify(other).and(Ok(()))
    }

    pub fn unify(&self, other: &Type) -> Result<Type> {
        if self == other {
            Ok(*self)
        } else if mem::discriminant(self) != mem::discriminant(other) {
            error(Error::TypeMismatch(*self, *other))
        } else {
            error(Error::InternalRhizomeError(
                "unreachable case in type checking".to_owned(),
            ))
        }
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Type::Bool => "bool",
            Type::S8 => "s8",
            Type::U8 => "u8",
            Type::S16 => "s16",
            Type::U16 => "u16",
            Type::S32 => "s32",
            Type::U32 => "u32",
            Type::S64 => "s64",
            Type::U64 => "u64",
            Type::Char => "char",
            Type::String => "string",
            Type::Cid => "CID",
            Type::Js => "Js",
        };

        f.write_str(s)
    }
}
