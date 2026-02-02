### Question 6: 

**Answer:** Add a `timezone` property set to `America/New_York` in the `Schedule` trigger.

I consulted the Kestra documentation for the Schedule trigger. To run a flow based on a specific timezone, the `timezone` property must be added directly under the trigger configuration:

```yaml
triggers:
  - id: daily
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 * * *"
    timezone: America/New_York
```