## DIABETES READMISSION PREDICTION USING PySpark

In this project I am using PySpark (2.3.0)'s latest ML library Spark.ML to predict whether a diabetic patient
would be readmitted in the future. I am using a dataset from UCI which can be [found here](https://archive.ics.uci.edu/ml/datasets/diabetes+130-us+hospitals+for+years+1999-2008).

The data set has over 1 lakh records of patients who underwent clinical care. The response variable is a 3 category variable having categories ">30" & "<30" which mean that the patient was readmitted and also indicates their age as well as "NO" for no readmission. I converted the problem to a binary classification problem indicating "Yes"/"No" for readmission.

The data had a lot of missing values encoded as "?". Spark 2.3.0 has a very high level interface called as DataFrames which are very flexible for data analysis, but still if we need a granular control over the data, we need to use RDDs. I had to use RDDs to map the "?" values to a Python None value.

After appropriate data cleaning, I used Logistic regression and Random Forests to fit a predictive model. My accuracy right now is **63%**.
