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

    subgraph Detailed Workflow
        A1[Load Conditions] --> A2[Load Tables]
        A2 --> A3[Define Variables]
        A3 --> A4[Create CustomLogger]
        A4 --> A5[Create Eligible]
        A5 --> A6[Eligible.__init__]
        A6 --> A7[Eligible.generate_eligibility]
        A7 --> A8[Eligible._create_work_tables]
        A8 --> A9[SQLConstructor.eligible.generate_work_table_sql]
        A9 --> A10[TeradataHandler.execute_query]
        A7 --> A11[SQLConstructor.eligible.generate_eligible_sql]
        A11 --> A12[TeradataHandler.execute_query]
        A12 --> A13[Waterfall.from_eligible]
        A13 --> A14[Waterfall.__init__]
        A14 --> A15[Waterfall.generate_waterfall]
        A15 --> A16[Waterfall._step1_create_base_tables]
        A16 --> A17[SQLConstructor.waterfall.generate_unique_identifier_details_sql]
        A17 --> A18[TeradataHandler.execute_query]
        A15 --> A19[Waterfall._step2_analyze_eligibility]
        A19 --> A20[Waterfall._calculate_unique_drops]
        A20 --> A21[SQLConstructor.waterfall.generate_unique_drops_sql]
        A21 --> A22[TeradataHandler.to_pandas]
        A19 --> A23[Waterfall._calculate_incremental_drops]
        A23 --> A24[SQLConstructor.waterfall.generate_incremental_drops_sql]
        A24 --> A25[TeradataHandler.to_pandas]
        A19 --> A26[Waterfall._calculate_regain]
        A26 --> A27[SQLConstructor.waterfall.generate_regain_sql]
        A27 --> A28[TeradataHandler.to_pandas]
        A19 --> A29[Waterfall._calculate_remaining]
        A29 --> A30[SQLConstructor.waterfall.generate_remaining_sql]
        A30 --> A31[TeradataHandler.to_pandas]
        A15 --> A32[Waterfall._step3_create_dataframes]
        A32 --> A33[Waterfall._step4_create_excel]
        A33 --> A34[Create Output from Waterfall]
        A34 --> A35[Output.__init__]
        A35 --> A36[Output.create_output_file]
        A36 --> A37[Output._save_output_file]
    end
```
