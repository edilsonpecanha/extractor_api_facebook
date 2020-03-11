# Facebook Marketing API extractor in Python saving in an S3 bucket.

<img src="https://scontent-sjc3-1.xx.fbcdn.net/v/t39.2178-6/851593_516881288424097_1568644600_n.jpg?_nc_cat=101&_nc_sid=5ca315&_nc_ohc=mymTrHnHE_sAX_N4WEm&_nc_ht=scontent-sjc3-1.xx&oh=7d12083c6a4f7bf1b5db66b4a26b047b&oe=5EA5F63B" class="center">

The Marketing API has three levels: campaign, ad set
and ad.
Each level increases the granularity of the information that can be extracted. In this script the level of ad was used.

### Prerequisites
You will need to install facebook-business with:
```
pip install facebook-business
```

You need to create an application on Facebook at https://developers.facebook.com/apps/ and add the Marketing API product.
The extractor will need the application id (ex .: 123456789012345), the secret key (ex .: e123c4f5b67dc890c1f0db2a111d2c3e), token (ex .: EAARxN4MwEpABCOi96sB7pCV1a93LnIAMvwkCaxcfDEFpWNZAW13kga6YkHPZAttFkGHIJK1pJy84ZBQAeeRPWMJRn3Mk4dJUKZBjjhGhADghoGx5zgb273cvMbG1VvIifhCWCjW2S4hEyS5X305F252k8QAqF6wv9P09pgl4JFlAZAZD) and account_id (ex .: act_123456789012345)
In the script it is possible to use more than one account.





