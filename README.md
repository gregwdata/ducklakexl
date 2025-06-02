## TODOs

- [ ] Local `xlsx` file
- [ ] OneDrive API calls async-ify
- [ ] Make it ACID (one-drive)?
    - Lazy way: 
        - A semafore strategy on a separate sheet
    - Better way: 
        - Abstract out an append-only SCD-style version of each ducklake table. 
        - Convert into/out of that to current version of each table as needed.
        - Robustness against case where new lines appended concurrently?
- [ ] Ensure functionality for SharePoint matches that on OneDrive 
- [ ] On push, only write changed tables
    - Cache ducklake tables on pull, compare before push to identify changes
    - More CDC/wal way to do it?
    - Just append instead of clear and write, where applicable