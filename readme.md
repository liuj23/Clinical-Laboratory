## Background: 
Demand forecasting is the area of predictive analytics devoted to predicting future volumes of services or consumables. Fair
understanding and estimation of how demand will vary facilitates the optimal utilization of resources. In a medical laboratory, accurate forecasting of future demand, that is, test volumes, can increase efficiency and facilitate long‑term laboratory planning. Importantly, in an era of utilization management initiatives, accurately predicted volumes compared to the realized test volumes can form a precise way to evaluate utilization management initiatives. Laboratory test volumes are often highly amenable to forecasting by time-series models; however, the statistical software needed to do this is generally either expensive or highly technical.

## Method
In this paper, we describe an open-source web-based software tool for time-series forecasting and explain how to use it as a demand forecasting tool in clinical laboratories to estimate test volumes.

## Results:
This tool has three different models, that is, Holt-Winters multiplicative, Holt-Winters additive, and simple linear regression. Moreover, these models are ranked and the best one is highlighted.

## Conclusion:
This tool will allow anyone with historic test volume data to model future demand.
## Software Requirements
* R
  * version: >= 3.4.4$
  * Libraries
    - shiny
    - tseries
    - forecast
    - png
    - DT
    - zoo
    - rsconnect
    - date
    - ggplot2

## Getting Started
1. Install prerequisites
2. Open R
3. run commands:
  - library(shiny)
  - runApp()
4. click: 'Open in Browser'
5. See 'Using the Software' section in publication.pdf

## Additional information
See publication.pdf
