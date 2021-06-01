const faker = require('faker');

const generateUser = ({
  firstName = faker.name.firstName(),
  lastName = faker.name.lastName(),
  department,
  createdAt = new Date()
} = {}) => ({
  firstName,
  lastName,
  department,
  createdAt
});

const generateArticle = ({
  name = faker.random.words(),
  description = faker.lorem.sentence(),
  type,
  tags = []
} = {}) => ({
  name,
  description,
  type,
  tags
})

module.exports = {
  mapUser: generateUser,
  getRandomFirstName: () => faker.name.firstName(),
  mapArticle: generateArticle
};
