#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BatchOp {
    Commit = 0,
    Revert = 1,
}

impl TryFrom<u8> for BatchOp {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Commit),
            1 => Ok(Self::Revert),
            _ => Err(value),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EntityEventType {
    Created = 0,
    Updated = 1,
    Deleted = 2,
    Expired = 3,
    Extended = 4,
}

impl TryFrom<u8> for EntityEventType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Created),
            1 => Ok(Self::Updated),
            2 => Ok(Self::Deleted),
            3 => Ok(Self::Expired),
            4 => Ok(Self::Extended),
            _ => Err(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_op_repr_values() {
        assert_eq!(BatchOp::Commit as u8, 0);
        assert_eq!(BatchOp::Revert as u8, 1);
    }

    #[test]
    fn entity_event_type_repr_values() {
        assert_eq!(EntityEventType::Created as u8, 0);
        assert_eq!(EntityEventType::Updated as u8, 1);
        assert_eq!(EntityEventType::Deleted as u8, 2);
        assert_eq!(EntityEventType::Expired as u8, 3);
        assert_eq!(EntityEventType::Extended as u8, 4);
    }

    #[test]
    fn batch_op_roundtrip() {
        assert_eq!(BatchOp::try_from(0u8).unwrap(), BatchOp::Commit);
        assert_eq!(BatchOp::try_from(1u8).unwrap(), BatchOp::Revert);
        assert!(BatchOp::try_from(2u8).is_err());
    }

    #[test]
    fn entity_event_type_roundtrip() {
        for val in 0u8..=4 {
            assert!(EntityEventType::try_from(val).is_ok());
        }
        assert!(EntityEventType::try_from(5u8).is_err());
    }
}
