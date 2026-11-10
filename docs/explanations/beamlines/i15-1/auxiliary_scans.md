# I15-1 Auxiliary Scans

I15-1 PDF experiments require some scans to be run before hand, for use during analysis. These are added to the queue automatically by the i15-1 queue plugin. For each experiment, we need:

- an air scan, at room temperature
- an empty capillary scan, matching the capillary type and temperature range to the experiment
- a standard sample scan, at room temperature, matching the capillary type to the experiment

These must happen in this order, as the analysis process is layered - i.e, the empty capillary scan needs the air scan to be processed, the standard sample needs the empty capillary, and therefore the air, and the actual experiment needs all three. Additionally, to save time, we can merge these auxiliary scans together, to avoid queuing up three additional scans for every experiment. For example, experiments can always share an air scan (as it's always the same); empty capillary scans can be shared as long as the temperature range spans the range of the experiment, and the capillary type is the same; and standard sample scans can be shared as long as the capillary type is the same.

The above requirements lead to the following logic:

- An experiment will require the three auxiliary scans outlined above, the details of which will be a function of the experiment parameters. See `I151Converter._get_required_auxiliary_scans`
- An `AuxiliaryScan` has a `requires` field, which details the other required auxiliary scans that must proceed it. The `AuxiliaryScan` objects produced by `I151Converter._get_required_auxiliary_scans` have each other as requirements. The empty capillary scan will be required by the standard sample scan, and the air scan will be required by both the other two.

To add auxiliary scans, we build a new queue that includes the tasks along with their auxiliary scans. For each task in the queue, we will:

- Gather the required auxiliary scans
- For each required auxiliary scan:
- Check whether a matching auxiliary scan already exists in the queue or tiled. For an existing scan to be considered suitable, all of the required auxiliary scan's own required auxiliary scans must also be required by the existing scan.
- If a suitable existing does not exist, we will see if we can combine the required auxiliary scan with an existing scan. This, for example, could involve expanding the temperature range of a queued auxiliary scan. Combining these auxiliary scans also combines the requirements of both scans, so no requirement information is lost, but we also don't duplicate similar requirements. We then go to the beginning of this checklist, and repeat for the required auxiliary scans of the combined auxiliary scan, inserting the results before the combined scan in the queue.
- If we cannot combine the required auxiliary scan, we will add the auxiliary scan to the queue. However, right before we do this, we will go through the required auxiliary scans of the auxiliary scan we added, and repeat this checklist for them. This ensures the auxiliary scan is added behind its requirements.
- Once all the recursive adding of required auxiliary scans is complete, we add the task itself to the new queue.
