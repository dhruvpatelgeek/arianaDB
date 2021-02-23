# How to compile and run 

#### Run

1 .````RUN_DATABASE.sh```` 

2 .````RUN_SERVER.sh````

in that order 

(if you dont run the database before the server it won't be able to store values)

#### TESTING

if you dont have your own testing suite 

you can run ````node test/test_problem_2b.js```` after running 

````RUN_DATABASE.sh```` 

````RUN_SERVER.sh````






make sure you have ports

```
3000<- store

3001<- client
```

available 

if not you can modify the  constants in````RUN_DATABASE.sh````  ,.````RUN_SERVER.sh```` files





- it will first test ```` "/" ````endpoint to  see if the server is up

- it will send 100 requests to ```` "/stats/" ```` and after the rate limiter kicks in you should see the number of rates limited (HTTP err code 429) go up
- it will send 100 requests to ```` "/view/" ```` you can see the store.go term window that the client is periodically sending key<->value pairs to the store,
  you can also see it in the server window that the client prints the store response.

