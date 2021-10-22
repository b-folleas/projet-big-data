# Big Data Project

## Get started

First of all, make sure you have Python installed on your device by checking your version

```sh
python --version
```
Or 
```sh
py --version
```

Otherwise, install it [there](https://www.python.org/downloads/)

Clone the project from his github repository

```sh
git clone 
```

Lauch the docker-compose which will run the postgres database

```sh
docker-compose up --build
```
Or execute `./startup.sh`

## Extensions modules

Make sure you have **Pip** (Python Package Manager) installed on your device by checking your version

```sh
pip --version
```

Before installing modules on your local environment, you can set up a virtual environment using **pipenv**

```sh
pip install pipenv
```

Make sure pipenv is correctly installed with 
```sh
pipenv --version
```

The following modules will be installed as needed within the project : requests, python-dotenv
```sh
pipenv install requests
pipenv install python-dotenv
```

To uninstall a module, use 
```sh
pipenv uninstall my_module
```

## Launch

You shoudl still be at the root of the project

Then, enter your virtual environment with 
```sh
pipenv shell
```

Finally, you can start the python project

```sh
python ./src/main.py
```

To exit pipenv shell just type
```sh
exit
```

## Recommendation

Recommendation is based on a combination metadata and semantic information through a random tree algorithme.  

To test it. Choose a user id (ex: 1) after starting the main. The recommendation is more precise and pertinent as the amount of data (number of history entries) is rich. 

## Visualization

The visualization of the data of our appication is made out of two tables and two graphs so far :

## Tests

Getting the recommendation result, we used functional testing to evaluate the relevancy of the recommendation system.

## Improvements

A lot of this project functionalities could and should be improved such as follow :

- Have a dynamic user profile creation
- Define tags for the user and give it a more effective weight for the recommendation algorithm
- A user should not see once again a painting already seen
- Have new visualization tools such as the accuracy of the recommendation algoritm based on a test set

## Contributors

Beldjilali Iliès, Bellet Éloi, Folléas Brice, Mittelette Nathan
