'use strict'

const {mapUser, getRandomFirstName, mapArticle} = require('./util')

// db connection and settings
const connection = require('./config/connection')
let userCollection, articleCollection, studentCollection

const getCollection = async(name) =>{
  await connection.get().dropCollection(name)
  await connection.get().createCollection(name)

  return connection.get().collection(name)
}

run()

async function run() {
  await connection.connect()

  userCollection = await getCollection('users')
  articleCollection = await getCollection('articles')
  studentCollection = await connection.get().collection('students')

  await example1()
  await example2()
  await example3()
  await example4()
  console.log("---------------Articles-------------------")
  await article1()
  await article2()
  await article3()
  await article4()
  await article5()
  console.log("---------------Students-------------------")

  await student1()
  console.log("------------------------------------------")
  await student4()
  await student6()

  await connection.close()
}

// #### Users

// - Create 2 users per department (a, b, c)
async function example1() {

  const departments = ['a', 'a', 'b', 'b', 'c', 'c'];
  const users = departments.map( d => ({department: d})).map(mapUser)

  try {
    const {result} = await userCollection.insertMany(users)
    console.log(`Added ${result.n} users`);
  } catch (err) {
    console.error(err)
  }
}

// - Delete 1 user from department (a)

async function example2() {
  try {
    const {result} = await userCollection.deleteOne({department: 'a'});
    console.log(`Removed ${result.n} user`)
  } catch (err) {
    console.error(err)
  }
}

// - Update firstName for users from department (b)

async function example3() {
  try {
    const usersB = await userCollection.find({department: 'b'}).toArray()
    const bulkWrite = usersB.map(user => ({
      updateOne: {
        filter: {_id: user._id},
        update: {$set: {firstName: getRandomFirstName()}}
      }
    }))

    const {result} = await userCollection.bulkWrite(bulkWrite)
    console.log(`Updated ${result.nModified} users`)
  } catch (err) {
    console.error(err)
  }
}

// - Find all users from department (c)
async function example4() {
  try {
    const users = await userCollection.find({department : 'c'}).project({_id: 0}).toArray();
    console.log('Users:')
    users.forEach( i => console.log(i))
  } catch (err) {
    console.error(err)
  }
}

// ##Articles

// - Create 5 articles per each type (a, b, c)
async function article1() {
  const types = ['a', 'b', 'c'];
  let typesObj = [] 
  for(let i = 0; i < 5; i++) typesObj = typesObj.concat((types.map( t => ({type: t}))))

  const articles = typesObj.map(mapArticle);
  try{
    const {result} = await articleCollection.insertMany(articles)
    console.log(`Added ${result.n} articles`);
  } catch(err){
    console.log(err)
  }
}

// - Find articles with type a, and update tag list with next value [‘tag1-a’, ‘tag2-a’, ‘tag3’]
async function article2() {
  const [find, update] = [{type: 'a'}, {$set : { tags : ['tag1-a', 'tag2-a', 'tag3']}}]
  try{
    const {result} = await articleCollection.updateMany(find, update);
    console.log(`Modified ${result.nModified} articles`)
  } catch(err){
    console.log(err);
  }
}

// - Add tags [‘tag2’, ‘tag3’, ‘super’] to other articles except articles from type a
async function article3() {
 
  const [find, update] = [{type: {$ne: 'a'}}, {$set : { tags : ['tag2', 'tag3', 'super']}}]
  try{
    const {result} = await articleCollection.updateMany(find, update);
    console.log(`Modified ${result.nModified} articles`)
  } catch(err){
    console.log(err);
  }
}

//- Find all articles that contains tags [tag2, tag1-a]
async function article4() {
  //const find = {tags: {$all:['tag2', 'tag1-a']}}
  const find = {tags: { $in: ['tag2', 'tag1-a'] }}
  try{
    const result = await articleCollection.find(find).count();
    console.log(`Found ${result} articles`)
  } catch(err){
    console.log(err);
  }
}

// - Pull [tag2, tag1-a] from all articles
async function article5() {
  const update = { $pull: { tags: { $in: [ "tag2", "tag1-a"] }}}
  try{
    const {result} = await articleCollection.updateMany({},update);
    console.log(`Pulled ${result.nModified} articles`)
  } catch(err){
    console.log(err);
  }
}

// ##Students

// - Find all students who have the worst score for homework, sort by descent
async function student1() {
  // it wasn't clear for me how to determine criterion for the worst score, so I consider score<30 to be bad
  try{
    const result = await studentCollection.aggregate(
     [
      {$unwind: '$scores' },
      {$match: {'scores.type': 'homework','scores.score': { $lt: 30} }},
      {$sort: {'scores.score': -1}},
     ]
    ).toArray();
    console.log(result);
  } catch(err){
    console.log(err);
  }
}


// - Calculate the average score for homework for all students

async function student4() {
 
  try{
    const result = await studentCollection.aggregate(
     [
      {$unwind: '$scores' },
      {$match: {'scores.type': 'homework'}},
      { $group: { _id: null, avg : { $avg : '$scores.score'}}}
     ]
    ).toArray();
    console.log(`Average score ${result[0].avg}`)
    
  } catch(err){
    console.log(err);
  }
}

// - Mark students that have quiz score => 80
async function student6() {
  try{
    const result = await studentCollection.aggregate(
     [
      {$unwind: '$scores' },
      {$match: {'scores.type': 'quiz','scores.score': { $gte: 80} }},
      {$addFields: {points_80: true}}
     ]
    );
  } catch(err){
    console.log(err);
  }
}

