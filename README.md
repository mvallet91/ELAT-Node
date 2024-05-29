# ELAT-Node
ELAT, the edX Log File Analysis Tool, processes edX data packages for analysis. ELAT-Node turns the data packages into a MongoDB, which can be used for further learner data analysis.

## Requirements
`node >= 11.14.0`

## Installation

### npm
```console
npm i
```

### Bun
```console
bun i
```


## Configuration
In the `main` function in `index.js`, the courses and working directory in which the data package can be found can be configured. `courses` is a list of the course identifiers, which ELAT-Node will try to find. The `workingDirectory` is the directory where the [edX data package](https://edx.readthedocs.io/projects/devdata/en/stable/internal_data_formats/package.html) can be found. It is also possible to configure an alternative database for testing purposes. This can be configured in the `main` function too. Make sure to change `dev` in `index.js` and `databaseHelpers.js` to `true`.

## Running the script

### Node
```console
node ./index.js
```

### Bun
```console
bun ./index.js
```
