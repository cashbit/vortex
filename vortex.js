/*
*   Vortex
*   local and remote queue management
 *
 *   See example
 *
 *   Yuo can create a communication queue between different meteor apps
 *
 *
*
*
* */

/*

 Example of tipical vortex object

 var obj = {
     _id: "sdlkjsdldkdsjl",
     readers: ["pippo","pluto,"paperino"],
     createAt : "30938398389308309839",
     from: "127.0.0.1",
     to: ["ciccio"],
     topic: "kart",
     operation: "insert",
     payload: {
         _id:"ekljlkjdkjdkljd",
         kartid: "3098309839038093839839803",
         customer: "093833093830",
         total: "345"
         }
 }

 */

// for the proxy: export HTTP_PROXY=http://proxy.siad.lcl:8080

vortex = function (collectionName,config,callback){

    var remoteConfig = config || {
            recipients : ["*"], // filter objects by destination, * is for ALL, or specify an array
            topics : ["*"] // optional, if omitted will catch all *
    } ;

    remoteConfig.topics = remoteConfig.topics || ["*"] ;
    remoteConfig.recipients = remoteConfig.recipients || ["*"] ;

    var fromRemoteCallBack = callback || function(){} // will be called for each new object in the queue

    if (remoteConfig.hostname){

        var remote = DDP.connect(remoteConfig.hostname);

        var vortexRemoteCollection = new Meteor.Collection(remoteConfig.remoteCollectionName, {connection:remote});

        remote.subscribe(
            remoteConfig.remoteCollectionName,
            {
                readers : {$ne:remoteConfig.recipients},
                recipient : {$in:remoteConfig.recipients},
                topic : {$in:remoteConfig.topics}
            },
            function() {
                console.log("subscribed to " + this.name + " on:",remoteConfig.hostname) ;
            }
        );

        vortexRemoteCollection.find().observe({
            added: function(obj) {

                //delete(obj._id) ;

                var exitentObj = vortexLocalCollection.findOne(obj._id) ;

                if (exitentObj){
                    vortexRemoteCollection.update(
                        obj._id,
                        {$addToSet:{readers:{$each:remoteConfig.recipients}}}
                    )
                    return ;
                }

                obj.fromHostName = remoteConfig.hostname ;
                delete(obj.workers) ;
                var addedObj = vortexLocalCollection.insert(obj) ;
                if (addedObj){
                    vortexRemoteCollection.update(
                        obj._id,
                        {$addToSet:{readers:{$each:remoteConfig.recipients}}}
                    )
                }
            }
        });

        remote.onReconnect = function () {
            console.log("RECONNECTING REMOTE VORTEX");

            if (!remoteConfig.loginConfig) return ;
            remote.call("login",remoteConfig.loginConfig,
                function(err,result) {
                    //Check result
                    if (err) return console.log("Login error",err) ;
                }
            );
        };
    }


    var vortexLocalCollection = new Meteor.Collection(collectionName);


    if (Meteor.isServer){

        vortexLocalCollection.allow({
            insert:function(userId,doc){
                if (!userId) return false ;
                doc.documentOwner = userId;
                doc.createdAt = new Date() ;
                return true ;
            },
            update: function(userId, doc, fieldNames, modifier){
                if (!userId) return false ;
                //modifier.$set.updatedAt = new Date() ;
                return true ;
            },
            remove: function(userId,doc){
                return doc.documentOwner == userId ;
            }
        }) ;

        Meteor.publish(collectionName, function (criteria) {
            return vortexLocalCollection.find(criteria);
        });
    }

    vortexLocalCollection.find({
        workers : {$ne:remoteConfig.recipients},
        recipient : {$in:remoteConfig.recipients},
        topic : {$in:remoteConfig.topics}
    }).observe({
        added: function(obj){
            var worked = fromRemoteCallBack(obj) ;
            if (worked) {
                vortexLocalCollection.update(
                    obj._id,
                    {$addToSet:{workers:{$each:remoteConfig.recipients}}}
                ) ;
            }
        }
    });

    return {
        add:function(payload,from,recipient,topic,operation){
            var obj = {
                recipient : recipient || "*",
                topic : topic,
                operation : operation,
                payload : payload,
                from : from || "*",
                sentOn: new Date()
            } ;
            return vortexLocalCollection.insert(obj) ;
        }
    }
}