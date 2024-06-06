# Workflow for Using the Library

```mermaid
graph TD
    A[Load Conditions] --> B[Load Tables]
    B --> C[Define Variables]
    C --> D[Create CustomLogger]
    D --> E[Create Eligible]
    E --> F[Generate Eligibility]
    F --> G[Create Waterfall from Eligible]
    G --> H[Generate Waterfall]
    H --> I[Create Output from Waterfall]
    I --> J[Set Output Instructions]
    J --> K[Create Output File]

    subgraph Initialization
        E1[Initialize Eligible]
        E2[Generate Eligibility]
        E --> E1
        E1 --> E2
    end

    subgraph Waterfall Generation
        G1[Initialize Waterfall]
        G2[Generate Waterfall]
        G --> G1
        G1 --> G2
    end

    subgraph Output Generation
        I1[Initialize Output]
        I2[Create Output File]
        I --> I1
        I1 --> I2
    end

    subgraph Eligible Class
        E1 --> E3[SQLConstructor]
        E3 --> E4[EligibilitySQLConstructor]
        E3 --> E5[TeradataHandler]
    end

    subgraph Waterfall Class
        G1 --> G3[SQLConstructor]
        G3 --> G4[WaterfallSQLConstructor]
        G3 --> G5[TeradataHandler]
    end

    subgraph Output Class
        I1 --> I3[SQLConstructor]
        I3 --> I4[OutputFileSQLConstructor]
        I3 --> I5[TeradataHandler]
    end
```
