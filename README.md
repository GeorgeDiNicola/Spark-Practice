# Apache Spark Practice
Practice aggregation and transformation operations using Apache Spark. The project uses large datasets of flight delays to demonstrate the capabilities of Spark in handling operations on big data distributed across multiple nodes. The project is designed to be run in a Docker container, making it easy to set up and run without the need for complex installations.

# How to Setup & Run the Project
```
make install
make run
make stop
```

# Dataset Source
29 million rows of flight data from 2018 to 2022, including details such as departure and arrival times, delays, cancellations, and diversions. The dataset is sourced from the [Kaggle Flight Delay Dataset](https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022)

# Flight Summaries Years 2018-2022 Output
| Airline | Total Flights | Cancelled Flights | Diverted Flights | Avg Departure Delay (min) | Avg Arrival Delay (min) | Total Departure Delay (min) | Total Arrival Delay (min) | Avg Distance | Cancellation Rate | Diversion Rate |
|---------|--------------:|------------------:|-----------------:|--------------------------:|------------------------:|----------------------------:|--------------------------:|-------------:|------------------:|---------------:|
| Air Wisconsin Airlines Corp | 380,202 | 14,092 | 818 | 15.23 | 15.98 | 5,583,145 | 5,836,745 | 354.42 | 0.0371 | 0.0022 |
| Alaska Airlines Inc. | 906,259 | 17,117 | 2,231 | 8.54 | 9.48 | 7,596,153 | 8,403,759 | 1,325.70 | 0.0189 | 0.0025 |
| Allegiant Air | 489,400 | 22,409 | 1,408 | 17.24 | 18.42 | 8,050,987 | 8,575,380 | 877.21 | 0.0458 | 0.0029 |
| American Airlines Inc. | 3,134,117 | 95,058 | 7,784 | 13.75 | 13.88 | 41,820,291 | 42,087,207 | 1,000.92 | 0.0303 | 0.0025 |
| Cape Air | 1,661 | 2 | 4 | 4.64 | 5.39 | 7,704 | 8,921 | 117.27 | 0.0012 | 0.0024 |
| Capital Cargo International | 392,011 | 15,481 | 1,163 | 10.88 | 11.71 | 4,104,360 | 4,396,869 | 283.09 | 0.0395 | 0.0030 |
| Comair Inc. | 957,220 | 31,795 | 2,478 | 12.79 | 13.16 | 11,850,267 | 12,143,439 | 408.45 | 0.0332 | 0.0026 |
| Commutair Aka Champlain Enterprises, Inc. | 260,048 | 11,005 | 814 | 22.74 | 24.01 | 5,670,145 | 5,960,728 | 386.42 | 0.0423 | 0.0031 |
| Compass Airlines | 154,985 | 1,986 | 232 | 13.85 | 14.40 | 2,119,040 | 2,199,463 | 538.26 | 0.0128 | 0.0015 |
| Delta Air Lines Inc. | 3,294,917 | 46,734 | 6,042 | 9.63 | 9.50 | 31,296,964 | 30,809,344 | 918.91 | 0.0142 | 0.0018 |
| Empire Airlines Inc. | 23,122 | 1,293 | 105 | 10.03 | 10.68 | 219,644 | 231,803 | 69.51 | 0.0559 | 0.0045 |
| Endeavor Air Inc. | 998,224 | 20,193 | 1,767 | 10.22 | 10.57 | 9,994,000 | 10,310,272 | 425.72 | 0.0202 | 0.0018 |
| Envoy Air | 1,072,778 | 36,123 | 2,676 | 10.44 | 11.77 | 10,838,459 | 12,168,056 | 468.98 | 0.0337 | 0.0025 |
| ExpressJet Airlines Inc. | 353,669 | 14,363 | 1,070 | 17.04 | 18.49 | 5,785,773 | 6,254,324 | 464.85 | 0.0406 | 0.0030 |
| Frontier Airlines Inc. | 570,452 | 13,730 | 795 | 17.96 | 17.78 | 10,003,561 | 9,886,001 | 993.88 | 0.0241 | 0.0014 |
| GoJet Airlines, LLC d/b/a United Express | 276,486 | 9,240 | 660 | 17.03 | 17.64 | 4,554,124 | 4,702,866 | 460.13 | 0.0334 | 0.0024 |
| Hawaiian Airlines Inc. | 310,782 | 3,146 | 278 | 5.41 | 6.01 | 1,664,619 | 1,848,715 | 800.33 | 0.0101 | 0.0009 |
| Horizon Air | 471,153 | 11,022 | 1,104 | 7.01 | 7.72 | 3,230,290 | 3,541,964 | 434.65 | 0.0234 | 0.0023 |
| JetBlue Airways | 1,106,079 | 29,095 | 3,524 | 20.24 | 20.03 | 21,814,001 | 21,501,306 | 1,142.54 | 0.0263 | 0.0032 |
| Mesa Airlines Inc. | 749,216 | 25,769 | 1,880 | 15.85 | 16.44 | 11,477,813 | 11,859,357 | 600.73 | 0.0344 | 0.0025 |
| Peninsula Airways Inc. | 2,783 | 431 | 59 | 21.15 | 22.48 | 49,988 | 51,539 | 792.00 | 0.1549 | 0.0212 |
| Republic Airlines | 1,283,704 | 40,930 | 2,944 | 10.84 | 12.00 | 13,486,546 | 14,874,026 | 570.17 | 0.0319 | 0.0023 |
| SkyWest Airlines Inc. | 3,159,683 | 72,587 | 9,798 | 13.86 | 14.25 | 42,811,335 | 43,865,959 | 504.37 | 0.0230 | 0.0031 |
| Southwest Airlines Co. | 5,474,339 | 171,408 | 10,340 | 11.87 | 10.26 | 62,948,682 | 54,277,250 | 750.07 | 0.0313 | 0.0019 |
| Spirit Air Lines | 836,694 | 18,150 | 1,585 | 13.76 | 13.99 | 11,269,462 | 11,433,185 | 1,017.25 | 0.0217 | 0.0019 |
| Trans States Airlines | 161,590 | 6,614 | 597 | 21.78 | 23.04 | 3,380,615 | 3,556,820 | 462.18 | 0.0409 | 0.0037 |
| United Air Lines Inc. | 2,354,538 | 47,061 | 6,109 | 13.70 | 13.97 | 31,616,719 | 32,140,098 | 1,191.61 | 0.0200 | 0.0026 |
| Virgin America | 17,670 | 433 | 84 | 10.90 | 11.95 | 187,977 | 204,995 | 1,475.19 | 0.0245 | 0.0048 |