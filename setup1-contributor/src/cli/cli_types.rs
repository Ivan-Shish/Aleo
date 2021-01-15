use clap::AppSettings;

pub type NameType = &'static str;

pub type AboutType = &'static str;

pub type DescriptionType = &'static str;

pub type RequiredType = bool;

pub type PossibleValuesType = &'static [&'static str];

pub type IndexType = u64;

pub type ArgumentType = (NameType, DescriptionType, PossibleValuesType, RequiredType, IndexType);

// Format
// "[flag] -f --flag 'Add flag description here'"
pub type FlagType = &'static str;

// Format
// (argument, conflicts, possible_values, requires)
pub type OptionType = (
    &'static str,
    &'static [&'static str],
    &'static [&'static str],
    &'static [&'static str],
);

pub type SubCommandType = (
    NameType,
    AboutType,
    &'static [ArgumentType],
    &'static [FlagType],
    &'static [OptionType],
    &'static [AppSettings],
);
