# congress2dataset

## Installation 

## Usage

### Stage 1: Downloading raw data 

Before compiling a dataset, you first need to obtain the raw data.
Assuming you are rebuilding the data from scratch, you will construct a local copy of the data in the following directory structure:

```
data/
    {congress number}/
        {bill type}-{bill number out of 6 digits}/
            src.html.gz
            texts.html.gz
            cbos/
                {2 digit version number}.pdf.gz
            texts/
                {2 digit version number}.txt.gz
                {2 digit version number}.pdf.gz
            votes/
                {chamber}-{roll call number out of 5 digits}.html.gz
```

### Stage 2: Parsing raw data

This stage transforms the raw data into a MongoDB database.


### Stage 3: Exporting data