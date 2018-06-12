# Knossos

Developed by ChesterLab @ Arizona State University: Civil Engineering and the Built Environment

The data cleaning and preparation module for Icarus, located at [Icarus](github.com/ChesterLab/Icarus).
Icarus simulates agent level travel throughout the course of a day, using 'Agent Based Model' data provided
by the Maricopa Association of Governments, which is currently not publicly available due to confidentiality
concerns. This project is currently not runnable without the data format it's built for, though in the near
future there are planned improvments focused on facilitating use of standardized input sources.

## Getting Started

Simply clone the repository. 
In the case you have access to the data, place it in a folder named 'Data' within the Knossos repo.

### Prerequisites

Currently requires Python 3.6.5, and is only tested on OSX 10.13 and RHEL 6.7, no compability with
any versions of Windows or guaranteed (or expected).

To install the packages the project depends on, navigate to the Knossos directory and enter

```
pip3 install -r requirements.txt
```

If you have a non-standard install of Python 3.6.5 or Pip, use the paths associated with your modifications.
On RHEL, if install fails due to administrator privlidges, attempt the following
```
pip3 install -r requirements.txt --user
```

## Authors

* **Austin Michne** - *Lead Developer* - [amichne](https://github.com/amichne)

## Acknowledgments

* Maricopa Association of Governments for ABM Model access
