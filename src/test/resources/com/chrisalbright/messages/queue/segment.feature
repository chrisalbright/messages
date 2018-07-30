Feature: A Segment can store data sequentially
  Segments store data in a file.

  Scenario: An empty scenario contains no data
    Given An Empty Segment
    When We read from an Empty Segment
    Then We get no data