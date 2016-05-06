.. -*- rest -*-
.. vim:syntax=rest

========
tapeworm
========

The package allows you to perform tape backups in Linux. It is tailored 
towards backing up very large data sets where traditional tape rotation 
schemes may not be appropriate.

The on-tape format is simply a bunch of tar files, allowing data to be 
restored with just basic Linux tools. Information about what has been backed 
up is stored in a combination of a database and plain text files.

Optionally, parity files (par format) can be generated for each tar archive. 
These can be used to verify the contents of the tar archive and repair errors.

Instead of performing full backups, the program can optionally include some 
random portion of the unchanged files in each backup. Every file will be 
gauranteed to have been backed up at least once in the specified time period, 
allowing a full restore to be performed without going back and loading every 
tape you have ever written (and allowing old tapes to be reused). The cost of 
performing a full backup is then essentially spread out over a long time span 
instead of being a sudden spike.
