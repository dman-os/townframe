use crate::interlude::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Date(pub jiff::civil::Date);

impl Date {
    pub fn new(year: i16, month: i8, day: i8) -> Option<Self> {
        jiff::civil::Date::new(year, month, day).ok().map(Self)
    }

    pub fn year(&self) -> i16 {
        self.0.year()
    }

    pub fn month(&self) -> i8 {
        self.0.month()
    }

    pub fn day(&self) -> i8 {
        self.0.day()
    }
}

impl std::fmt::Display for Date {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{:04}-{:02}-{:02}",
            self.0.year(),
            self.0.month(),
            self.0.day()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Status {
    Unmarked,
    Pending,
    Cleared,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CommoditySide {
    Left,
    Right,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AmountStyle {
    pub commodity_side: CommoditySide,
    pub commodity_spaced: bool,
    pub decimal_mark: Option<char>,
}

impl Default for AmountStyle {
    fn default() -> Self {
        Self {
            commodity_side: CommoditySide::Left,
            commodity_spaced: false,
            decimal_mark: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AmountCost {
    Unit(Amount),
    Total(Amount),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostBasis {
    pub cost: Option<Box<Amount>>,
    pub date: Option<Date>,
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Amount {
    pub commodity: String,
    pub quantity: String,
    pub style: AmountStyle,
    pub cost: Option<Box<AmountCost>>,
    pub cost_basis: Option<CostBasis>,
}

impl Amount {
    pub fn is_missing(&self) -> bool {
        self.quantity.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PostingType {
    Regular,
    Virtual,
    BalancedVirtual,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BalanceAssertion {
    pub amount: Amount,
    pub is_total: bool,
    pub is_inclusive: bool,
}

pub type Tag = (String, String);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Posting {
    pub status: Status,
    pub account: String,
    pub amount: Amount,
    pub posting_type: PostingType,
    pub assertion: Option<BalanceAssertion>,
    pub comment: String,
    pub tags: Vec<Tag>,
    pub date: Option<Date>,
    pub date2: Option<Date>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Transaction {
    pub date: Date,
    pub date2: Option<Date>,
    pub status: Status,
    pub code: Option<String>,
    pub description: String,
    pub comment: String,
    pub tags: Vec<Tag>,
    pub postings: Vec<Posting>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PriceDirective {
    pub date: Date,
    pub commodity: String,
    pub price: Amount,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AccountType {
    Asset,
    Liability,
    Equity,
    Revenue,
    Expense,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountDeclaration {
    pub name: String,
    pub account_type: Option<AccountType>,
    pub comment: String,
    pub tags: Vec<Tag>,
}
