'use strict';
//RPUSH ClientCommands '{"dest":"*","command":{"command":"registerClient","params":{"foo":"bar"}}}'
var config=require(__dirname+'/config.js');
var CLIENT_HASH_NAME=config.CLIENT_HASH_NAME;
var CLIENT_COMMANDS_QUEUE=config.CLIENT_COMMANDS_QUEUE;
var REGISTRATION_QUEUE=config.REGISTRATION_QUEUE;
var SPARKS_SET=config.SPARKS_SET;
var CLIENT_PRESENCE_SUBSCRIBERS=config.CLIENT_PRESENCE_SUBSCRIBERS;
var Primus = require('primus')
  , http = require('http');

var log=require('winston');
log.add(log.transports.File, {level:"debug",timestamp:"true",filename:__dirname+'/logs/primus_server.log',maxsize:'50000000'});

log.info("primus_server started");

var primus=Primus.createServer(
 { port: config.PORT, transformer: 'engine.io',
   root:'/etc/ssl',
   cert:'star.internal.ausl.bologna.it.crt',
   key:'star.internal.ausl.bologna.it.key',
   ca:'star.internal.ausl.bologna.it.cabundle'
});

var Redis = require("redis"),
    redis = Redis.createClient(6379,'localhost',{retry_max_delay:2000});
var redisq=Redis.createClient(6379,'localhost',{retry_max_delay:2000});
var redisr=Redis.createClient(6379,'localhost',{retry_max_delay:2000});



function init(){

redis.on("error", function (err) {

        log.error("Redis error handler: " + err,err.stack);
    });

redisq.on("error", function (err) {

        log.error("Redisq error handler: " + err,err.stack);
    });

redisr.on("error", function (err) {
	 log.error("Redisr error handler: " + err,err.stack);
	    });


primus.on('connection', function (spark) {
  log.info('connection was made from', spark.address);
  log.debug('connection id', spark.id);

  spark.on('data', function (data) {
    log.debug('received data from the client', data);
    if (data.command==='registerClient' && data.params.user){
	  log.debug("Going to register client : ",spark.id);
	  data.params.sparkId=spark.id;
  	  redis.rpush(REGISTRATION_QUEUE,JSON.stringify({action:'register',data:data.params}));
			//registerClient(spark,data.params);
	}
    else if(data.command==="subscribeClientPresence") {
        log.debug("Client ",spark.id," subscribed for ClientPresence")
        redis.hset(CLIENT_PRESENCE_SUBSCRIBERS,spark.id,spark.id);
    }
    else if(data.command==="unsubscribeClientPresence") {
        log.debug("Client ",spark.id," UNsubscribed for ClientPresence")
        redis.hdel(CLIENT_PRESENCE_SUBSCRIBERS,spark.id);
    }
    else{
	  log.warn("command : ",data.command);
	}

  });

  //spark.write({message:'pippo'});
});

primus.on('disconnection',function(spark){
	redis.rpush(REGISTRATION_QUEUE,JSON.stringify({action:'unregister',data:spark.id}));
	//unregisterClient(spark);

});
redis.del(SPARKS_SET);
redis.del(CLIENT_HASH_NAME);
redis.del(CLIENT_PRESENCE_SUBSCRIBERS);
redisq.del(CLIENT_COMMANDS_QUEUE,function(err,reply){
readClientCommands();
});
redisr.del(REGISTRATION_QUEUE,function(err,reply){
handleRegistrationQueue();
});


//CATCH DI TUTTE LE ECCEZIONI
process.on('uncaughtException', function(err) {
  log.error('Caught exception: ' + err,err.stack);
});



}


function writeClient(user,app,message)
{
	if (app==null) {app='*';}

	log.info("Writing to:", user, " app:", app, " message:", message);
	log.info("CLIENT_HASH_NAME:", CLIENT_HASH_NAME);
	redis.hget(CLIENT_HASH_NAME,user,function(err,reply){
		log.info("Writing to ",user, " in callback");
		if (err){log.error(err);return;}
		if (reply==null){log.warn("WriteClient client ",user," not found!");return;}
        var sparks=JSON.parse(reply);

        for (var i=0;i<sparks.length;i++){
            if (app==='*' || sparks[i].application===app){
				var spark=primus.spark(sparks[i].sparkId);
				if (spark){
					spark.write(message);
				}

				}
         }
	});


}


function readClientCommands(){
    redisq.blpop(CLIENT_COMMANDS_QUEUE,0,function(err,reply){

	if (err != null) {
        log.error("Error: ",err);

    }
	if (reply!=null){
	        log.debug("reply",reply[1]);
		try{
			var command=JSON.parse(reply[1]);
		}
		catch (e){
		 log.error("Errore parsando comando JSON");
		 process.nextTick(readClientCommands);
		 return;
		}
		//tutto a tutti
		if (command.dest=="*"){
                    if (command.dest_app==null || command.dest_app=='*'){
			//tutto a tutti
                        primus.write(command.command);
                    }
                    else{
                        //tutti quelli di un'app
                        redisq.hgetall(CLIENT_HASH_NAME,function(err,reply){
                            if (err){log.error(err);return;}
                            if (reply==null){log.warn("No clients found!");return;}
			    log.info("reply:", reply);
                            for (var user in reply){
				log.info("user:", user);
                                writeClient(user,command.dest_app,command.command);
                            }

                        });
                    }
		}
		else if(command.dest!=null) {
			for (var i=0;i<command.dest.length;i++){
				var user=command.dest[i];
				log.debug("DEST: ",user);
				writeClient(user,command.dest_app,command.command);
			}


		}

	}
    if (err!=null){
            redisq.del(CLIENT_COMMANDS_QUEUE);
            redisq.once('connect',readClientCommands);
    }
    else {
        process.nextTick(readClientCommands);
    }
});
}


function handleRegistrationQueue(){
    redisr.blpop(REGISTRATION_QUEUE,0,function(err,reply){

	if (err != null) {
        log.error("Error: ",err);

    }
	if (reply!=null){
	    log.debug("handleRegistrationQueue reply",reply[1]);
		var command=JSON.parse(reply[1]);
		if (command.action==="register"){
			//REGISTER
			registerClient(command.data)
		}
		else if (command.action==="unregister"){
			//UNREGISTER
			unregisterClient(command.data)
		}
		else{
			//ERRORE
			log.error("UNSUPPORTED operation ",command.action," in handleRegistrationQueue !");
            process.nextTick(handleRegistrationQueue);
		}

	}
    if (err!=null){
            redisr.del(REGISTRATION_QUEUE);
            redisr.once('connect',handleRegistrationQueue);
    }
   // else {
       // process.nextTick(handleRegistrationQueue);
    //}
});
}


function registerClient(data){

	redis.hget(CLIENT_HASH_NAME,data.user,function(err,reply){
	if (reply==null){
            var sparks=[];
            //data.sparkId=spark.id;
            sparks.push(data);
		    redis.hset(CLIENT_HASH_NAME,data.user,JSON.stringify(sparks),function(err,reply){notifyClientPresenceSubscribers();});
                    redis.hset(SPARKS_SET,data.sparkId,data.user);
		    log.info("Client ",data.user,data.sparkId," registered");
            process.nextTick(handleRegistrationQueue);
	}
	else{
        	redis.hget(SPARKS_SET,data.sparkId,function(err,setreply){
            if (setreply){
		    log.info("Client ",data.user,data.sparkId," already registered");
            process.nextTick(handleRegistrationQueue); 
            }
            else{
                var sparks=JSON.parse(reply);
                //data.sparkId=spark.id;
                sparks.push(data);
                redis.hset(CLIENT_HASH_NAME,data.user,JSON.stringify(sparks),function(err,reply){notifyClientPresenceSubscribers();});
                redis.hset(SPARKS_SET,data.sparkId,data.user);
                log.info("Client ",data.user,data.sparkId," registered");
                process.nextTick(handleRegistrationQueue);
            }
        });
       	}
  });
}


function unregisterClient(sparkId){
    redis.hdel(CLIENT_PRESENCE_SUBSCRIBERS,sparkId);
    redis.hget(SPARKS_SET,sparkId,function(err,user){
            if (user==null){
            log.warn("Client ",sparkId," was not registered");
            process.nextTick(handleRegistrationQueue);
            }
            else{
                redis.hdel(SPARKS_SET,sparkId);
                redis.hget(CLIENT_HASH_NAME,user,function(err,userdata){
                        if (userdata){
                                var sparks=JSON.parse(userdata);
                                var newsparks=null;
                                for (var i=0;i<sparks.length;i++){
                                    if (sparks[i].sparkId==sparkId){
                                      newsparks=sparks.splice(i,1);
                                      break;
                                     }
                                 }
                                if (newsparks){
                                if (sparks.length!==0){
                                        //aggiorno gli sparks
                                        redis.hset(CLIENT_HASH_NAME,user,JSON.stringify(sparks),function(err,reply){notifyClientPresenceSubscribers();});
                                    }
                                else{
                                        //rimuovo l'utente che non ha piu' connessioni
                                        redis.hdel(CLIENT_HASH_NAME,user,function(err,reply){notifyClientPresenceSubscribers();});
                                }
                                log.info("Client ",user,sparkId," unregistered");
                                process.nextTick(handleRegistrationQueue);                    
                                }
                                else{
                                log.warn("Strange spark",sparkId," not found for user",user);
                                process.nextTick(handleRegistrationQueue);
                                }
                            }
                            else{
                                log.warn("Strange couldn't find userdata for :",user+sparkId);
                                process.nextTick(handleRegistrationQueue);
                            }

                });
            }
        });

}


function notifyClientPresenceSubscribers(){
    redis.hkeys(CLIENT_PRESENCE_SUBSCRIBERS,function(err,reply){
        if(err){log.error("Error reading CLIENT_PRESENCE_SUBSCRIBERS");return;}
        for (var r in reply){
            var spark=primus.spark(reply[r]);
            if(spark){
                spark.write({command:"refreshClientPresence"});
            }
        }

    }
    );

}


init();
