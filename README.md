# simple-database
This was a project for a Databases class I took in Winter 2020, where
we had to build a simple database from scratch. There were four parts to it,
each focusing on a different part of the database. It was a partner class.
Most of the code here was not ours, but was given to us. One of the hardest 
parts of this project was being introduced to such a large amount of code so
quickly, and then using the different parts in the sections we wrote
ourselves. It was a great way to model what coding on a development team is like,
where there is so much code that it sometimes takes time to find what you are 
looking for, which was a great learning experience.

We coded the data structures for Tuples, Relations/Tables, and paging, 
particularly caching pages. We then implementing filters and joins, aggregates, 
the ability to delete and insert tuples and have these operations reflected 
in the pages of a heap file, as well as a fun challenge in implementing an 
eviction policy for a buffer pool page cache. For the buffer pool page eviction 
policy, we decided to go with LRU or "Least Recently Used". 

We worked on join orderings, where we used statistics collected 
with histograms to determine which orderings of joins would be cost optimal. 
This included coding all of IntHistogram, TableStat, and JoinOptimizer.
We also made a new type of join and tried to figure it into our optimizer, HashEquiJoin.
Lastly, we created a locking protocol for our database, which we decided to
do on the level of pages. Every time a page is needed, to be either read or 
written to, we need to get a lock. 

Overall, this project was a very exciting way to become acquainted to coding with a large
amount of files and to working with a partner.
