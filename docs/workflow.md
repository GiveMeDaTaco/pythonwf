# Workflow for Using the Library

```mermaid
graph TD
    A[Eligible] --> B[Waterfall]
    B --> C[Output]

    subgraph Initialization
        A1[Initialize Eligible]
        A2[Generate Eligibility]
        A --> A1
        A1 --> A2
    end

    subgraph Waterfall Generation
        B1[Initialize Waterfall]
        B2[Generate Waterfall]
        B --> B1
        B1 --> B2
    end

    subgraph Output Generation
        C1[Initialize Output]
        C2[Create Output File]
        C --> C1
        C1 --> C2
    end

    subgraph Eligible Class
        A1 --> A3[SQLConstructor]
        A3 --> A4[EligibilitySQLConstructor]
        A3 --> A5[TeradataHandler]
    end

    subgraph Waterfall Class
        B1 --> B3[SQLConstructor]
        B3 --> B4[WaterfallSQLConstructor]
        B3 --> B5[TeradataHandler]
    end

    subgraph Output Class
        C1 --> C3[SQLConstructor]
        C3 --> C4[OutputFileSQLConstructor]
        C3 --> C5[TeradataHandler]
    end
```
