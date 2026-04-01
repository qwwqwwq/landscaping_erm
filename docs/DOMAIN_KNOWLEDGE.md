# GEM Database Export — Domain Knowledge

## Business Context

- **Company:** Best Buy In Town
- **Website:** www.bestbuybark.com
- **Address:** 2200 NE Cornelius Pass Road, Hillsboro, OR 97124
- **Phone:** 503-645-6665
- **Industry:** Landscaping supply — bulk bark, hemlock, soil, rock, yard debris recycling
- **Tax:** Oregon has no sales tax (confirmed by SALESTAX table: only record is "No Tax" rate 0)
- **Software version:** v2022.11.03.226

## Software: GEM by Sawyer Networks

- **Vendor:** Sawyer Networks (sawyernetworks.com)
- **Product:** GEM — a retail POS, inventory management, and accounting system
- **Database:** Microsoft SQL Server (`dbo.` schema prefix on all tables, plus `ssi.` for system tables)
- **Export:** ~400 tables dumped to CSV

## Data Volumes (rows, excluding header)

| Table | Rows | Description |
|-------|------|-------------|
| ITEMSOLD | 1,196,535 | Invoice payment summaries |
| PINVOICE | 1,177,422 | Pending/purchase invoice headers |
| AR_TRN | 192,662 | Accounts receivable transactions |
| DEPOSITS | 155,212 | Customer deposits/prepayments |
| ORDERS | 80,406 | Sales orders |
| HOURS | 78,682 | Employee clock in/out |
| CUSMER | 67,393 | Customer master records |
| CHECKS | 44,661 | Check/cash payments received |
| INVDET | 30,272 | Invoice line items (detail) |
| DAILY | 16,824 | Daily sales summaries |
| AR_HST | 3,953 | AR history |
| RETURNS | 4,116 | Product returns |
| ITEMS | 2,591 | Inventory items |
| APVEND | 615 | Vendors |
| CATEGORY | 294 | Product categories |
| GIFTCERT | 189 | Gift certificates |
| STOCK | 152 | Stock/inventory receipts |
| GLHeader | 97 | GL journal entry headers |
| STATION | 25 | POS stations/registers |
| COMPNY | 3 | Company info |
| SETUP | 2 | System configuration |

### Empty Tables (header only, module not used)
EMPLOYEE, GLACNT, GLDETAIL, LOCATION, Jobs, JobExpenses, JobEstimates, TIMESLIP, TRANSFER

## Date Encoding

**All dates are stored as Clarion date serial numbers** — an integer day count where **day 1 = December 28, 1800**. This is the native date format of the Clarion/TopSpeed development platform that GEM is built on.

**Conversion formula (Python):**
```python
from datetime import date, timedelta
real_date = date(1800, 12, 28) + timedelta(days=serial_value - 1)
```

**Verified calibration points:**
| Serial | Real Date | Source |
|--------|-----------|--------|
| 73082 | 2001-01-29 | First DAILY record |
| 81030 | 2022-11-03 | Matches software version date |
| 81890 | 2025-03-12 | Last DAILY record |

**Data spans from ~January 2001 to March 2025.**

## Core Table Schema

### COMPNY — Company Info
`COM_ID`, `COM_Name`, `COM_Mad1/2` (mailing address), `COM_MCit`, `COM_MSta`, `COM_MZip`, `COM_Vphn`, `COM_FPhn`, `COM_EMail`, `COM_WWWAddr`, `COM_BusType`, `COM_Version`

### CUSMER — Customer Master (67K rows)
- `CUS_CustID` — unique customer ID
- `CUS_CODE` — customer code (phone-number based)
- `CUS_NAME`, `CUS_FNAM`, `CUS_LNAM` — name fields
- `CUS_PHN1/2` — phone numbers
- `CUS_BAD1/2`, `CUS_BCIT`, `CUS_BSTA`, `CUS_BZIP` — billing address
- `CUS_TXID` — tax ID
- `CUS_SALP` — salesperson code (links to SALESID)
- `CUS_TERM` — payment terms (COD, VISA, HOUSE, etc.)
- `CUS_CLIM` — credit limit
- `CUS_TYPE` — P=personal, B=business
- `CUS_EMAIL`, `CUS_PRICEID`, `CUS_PERMDISC` — pricing/discount

### ITEMS — Inventory Items (2.6K rows)
- `ITE_INVNO` — item number
- `ITE_DESCRIPTION`, `ITE_LONGDESC` — descriptions
- `ITE_BARCODE` — barcode
- `ITE_CATG` — category name
- `ITE_C_ID` — category ID (links to CATEGORY)
- `ITE_TYPE` — I=inventory
- `ITE_SB`/`ITE_PB` — sell-by/purchase-by unit of measure
- `ITE_DQUAN` — default quantity
- `ITE_Dept_ID`, `ITE_DeptName` — department

Products include: bark (Pebble, Red Hemlock, Brown Hemlock), soil, rock, yard debris. Sold in yards, 5-gallon, and bulk (7.5 yard) units.

### ORDERS — Sales Orders (80K rows)
- `ORD_ID`, `ORD_INVOICENO` — identifiers
- `ORD_STATUS` — A=active, I=inactive
- `ORD_CASH`, `ORD_CHECK`, `ORD_CHARGECARD`, `ORD_ONACCOUNT` — payment split
- `ORD_TOTAL` — order total
- `ORD_SALESDATE` — integer date
- `ORD_CUSTID` — customer ID
- `ORD_SALESID` — salesperson code
- `ORD_NAME` — customer name ("CASH SALE" for walk-ins)
- `ORD_TAX1/2` — tax amounts
- `ORD_STATION` — POS station
- `ORD_JobNumber`, `ORD_DivisionID`

### INVDET — Invoice Line Items (30K rows)
- `IND_INVNO` — item number
- `IND_INVOICENO` — invoice number
- `IND_DESCRIPTION` — item description
- `IND_QUANTITY`, `IND_PRICE`, `IND_ITEMCOST` — qty/price/cost
- `IND_CUSTID` — customer
- `IND_TAX1`, `IND_DISC` — tax and discount
- `IND_SALES1/2` — salesperson codes
- `IND_COMM` — commission
- `IND_MARKUP` — markup
- `IND_Department`, `IND_UM` (unit of measure), `IND_CatID`

### ITEMSOLD — Invoice Payment Summaries (1.2M rows)
- `ITS_INVOICENO` — invoice number
- `ITS_TYPE` — I=Invoice
- `ITS_SALESDATE`, `ITS_SalesTime` — when
- `ITS_SOLDFOR`, `ITS_COST` — amounts
- `ITS_CASH`, `ITS_CHECK`, `ITS_CHARGECARD`, `ITS_DebitCard`, `ITS_GIFT`, `ITS_ONACCOUNT` — payment method breakdown
- `ITS_STATIONNO`, `ITS_STATUS`, `ITS_CUSTID`, `ITS_S_ID`

### PINVOICE — Pending Invoices (1.18M rows)
- `PNV_INVOICENO`, `PNV_TOTAL`, `PNV_SALESDATE`
- `PNV_CUSTID`, `PNV_SALESID`, `PNV_SALESNAME`
- `PNV_STATUS` — I=invoiced, V=void
- `PNV_COST`, `PNV_Tax`, `PNV_STATIONNO`

### AR_TRN — Accounts Receivable (193K rows)
- `ART_TYPE` — CHG=charge, PMT=payment, CRM=credit memo, etc.
- `ART_CustID` — customer
- `ART_TTOT` — transaction total
- `ART_PAID`, `ART_BALA` — paid amount, balance
- `ART_OPCL` — open/closed status (P=paid)
- `ART_TERMS` — payment terms (HOUSE, COD, etc.)
- `ART_INVOICENO`, `ART_SalesID`, `ART_GLAccount`

### DAILY — Daily Sales Summary (16.8K rows)
- `DAI_DATE` — integer date
- `DAI_AMOUNT` — daily sales
- `DAI_RAMOUNT` — returns
- `DAI_NUMBER` — transaction count
- `DAI_COST` — cost of goods
- `DAI_H1`–`DAI_H24` — hourly sales amounts
- `DAI_N1`–`DAI_N24` — hourly transaction counts
- `DAI_YTD`, `DAI_TAX1/2`, `DAI_DivisionID`

### SALESID — Employees / Sales Reps
- `SAL_SALESID` — 3-letter code (e.g., PRW, DDC, BJC)
- `SAL_SALESNAME` — full name
- `SAL_DEPT`, `SAL_Active`, `SAL_Access`
- `SAL_SaleLimit`, `SAL_ReturnLimit`
- **Note:** Contains plaintext passwords — handle with care

### HOURS — Employee Time Clock (79K rows)
- `HOU_SALESID` — employee code (links to SALESID)
- `HOU_DATE` — integer date
- `HOU_TIMEIN`, `HOU_TIMEOUT` — clock times (also integer-encoded)
- `HOU_TYPE` — 0=regular

### DEPTMENT — Departments
Lookup table: Yard Debris Recycle, Bulk Products, Tools, Dec Rock U-Haul, Miscellaneous, Fertilizers, etc.

### CATEGORY — Product Categories (294 rows)
- `CAT_CATEGORYDESC` — e.g., "Bark Nuggets - UH", "Hemlock - UH", "FREIGHT"
- `CAT_PRICE1`–`CAT_PRICE6` — price levels
- `CAT_MU1/2/3` — markup percentages (~40%/30%)
- `CAT_TAXID` — 1=taxable, 2=non-taxable

### APVEND — Vendors (615 rows)
- `VEN_V_ID`, `VEN_NAME`, `VEN_ADDR`, `VEN_PHON`
- `VEN_IS1099`, `VEN_CLMT` (credit limit)
- Suppliers include rock distributors, equipment vendors in the Portland/Vancouver area

### CHECKS — Payments Received (45K rows)
- `CHE_NAME`, `CHE_AMOUNT`, `CHE_DATE`, `CHE_NUMBER`
- `CHE_CUSTID`, `CHE_INVOICENO`, `CHE_TYPE` (I=invoice payment)

### DEPOSITS — Customer Deposits (155K rows)
- `DEP_INVOICENO`, `DEP_AMOUNT`, `DEP_DATE`
- `DEP_TYPE` — DBT=debit, Check, etc.
- `DEP_CUSTID`, `DEP_OrderNo`

### RETURNS — Product Returns (4.1K rows)
- `RTN_INVOICENO`, `RTN_SALESID`, `RTN_REASON`
- `RTN_DESC`, `RTN_COST`, `RTN_QUAN`
- Reasons: "Customer Changed Mind", "Product Return", "Pallet Deposit Refund"

## Transaction Flow

```
PINVOICE/PINVDET (pending/draft invoices)
        ↓
INVDET (finalized invoice line items) + ITEMSOLD (payment summary)
        ↓
AR_TRN (accounts receivable ledger)
        ↓
CHECKS / DEPOSITS (payments received)
```

## Multi-Site / Replication Fields

All tables include `_GUID`, `_Site`, and `_PrimaryID` columns. The `_Site` value is consistently `$$$$`, indicating a **single-site installation**. These are part of GEM's multi-site replication infrastructure but are not actively used here.

## Known Data Quality Issues

1. **Date encoding** — ✅ RESOLVED: Clarion dates (day 1 = 1800-12-28), see "Date Encoding" section
2. **Embedded newlines** — SHIPTO and APVEND CSV files contain newlines within address fields, making naive CSV parsing unreliable
3. **Plaintext passwords** — SALESID table contains SAL_PASSWORD in plaintext
4. **Large negative values in SCOMM** — Commission table has entries with very large negative quantities/prices that may be bulk adjustments or data artifacts
5. **TRANS table nearly empty** — Only 1 row despite being a "transactions" table; actual transaction data lives in ITEMSOLD/INVDET/ORDERS
