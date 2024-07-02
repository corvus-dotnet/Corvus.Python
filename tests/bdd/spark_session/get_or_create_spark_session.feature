Feature: Spark Session

    Scenario: Local Spark Session creation
        Given I use the default LocalSparkSessionConfig configuration
        When I create a Spark Session
        Then the Spark Session should be created and valid

    Scenario: Local Spark Session table writing
        Given I create a Spark Session with the default LocalSparkSessionConfig configuration with the name "bdd_test"
        When I write an empty DataFrame to a Delta table named "test_table"
        Then the table should exist in the default database
        And the table path should exist
