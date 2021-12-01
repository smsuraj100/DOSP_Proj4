#load "Utils.fsx"

#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open Utils
open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp

let args = System.Environment.GetCommandLineArgs()

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            log-config-on-start : on
            stdout-loglevel : DEBUG
            loglevel : ERROR
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                debug : {
                    receive : on
                    autoreceive : on
                    lifecycle : on
                    event-stream : on
                    unhandled : on
                }
            }
            remote {
                helios.tcp {
                    port = 7887
                    hostname = localhost
                }
            }
        }")

let system = ActorSystem.Create("RemoteFSharp", configuration)
let mutable retweetsCount = 0
let mutable disConnectedUsers = 0

type ClientManagerInitMessage = {
    NumClients: int;
} 

type ClientInitMessage = {
    MyId: int;
    MyTweetCount: int;
    NumClients: int;
    Disconnect: bool;
}

type ClientCompletionMessage = {
    ClientCompleted: bool;
}

type ForceFollowUser = {
    ForceFollowUserId: int;
}

type ClientManagerCompletionMessage = {
    ClientManagerCompleted: bool;
}

type SimulateTweetMessage = {
    SimulateTweet: bool;
}

type PopularityTeller = {
    Popularity: string;
}

type Client() = 
    inherit Actor()
    let mutable myId = 0
    let mutable myTweetCount = 0
    let mutable numClients = 0
    let mutable onePercent = 0
    let mutable disconnect = false
    override x.OnReceive (message:obj) =
        match message with
        | :? ClientInitMessage as msg ->
            myId <- msg.MyId
            myTweetCount <- msg.MyTweetCount
            numClients <-msg.NumClients
            disconnect <- msg.Disconnect
            let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")
            server.Tell { UserId = myId; } // Registering account
        | :? ForceFollowUser as msg ->
            let userId = msg.ForceFollowUserId
            let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")
            server.Tell { SelfId = myId; UserId = userId; }
        | :? Utils.RegistrationConfirmation as msg ->
            printfn "Registration Successful for user: user%d" myId
        | :? Utils.UpdateUserFeed as msg ->
            let random = new Random()
            let randomNumber = random.Next(10)
            if (randomNumber = 6 && msg.IsRetweet = false) then
               retweetsCount <- retweetsCount + 1
               let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")
               server.Tell { UserId = myId; Tweet = msg.Tweet; IsRetweet=true }
        | :? Utils.AcknowledgementOfTweet as msg ->
            if msg.IsRetweet = false then
                x.Self.Tell { SimulateTweet = true }
        | :? SimulateTweetMessage as msg ->
            if myTweetCount <> 0 && disconnect = false then
                let random = new Random()
                let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")
                let mutable tweet = ("tweet_" + string(myTweetCount))
                if random.Next(2) = 1 then
                    tweet <- (tweet + "@" + string(random.Next(1,numClients+1)))
                if random.Next(2) = 1 then
                    tweet <- (tweet + "#" + string(random.Next(1,10)))
                server.Tell { UserId = myId; Tweet = tweet; IsRetweet=false }
                myTweetCount <- myTweetCount - 1 
            else
                if disconnect = true then
                    let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")
                    server.Tell {IsUserInactive = myId}
                let clientManager = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:7887/user/ClientManager")
                clientManager.Tell { ClientCompleted = true }
        | _ -> printfn "ERROR WHILE PARSING MESSAGE"
        
type ClientManager() =
    inherit Actor()
    let mutable originalSender = null
    let mutable numClients = 0
    let mutable completedClients = 0
    let mutable startTime = 0.0
    let mutable endTime = 0.0
    let mutable totalTweets = 0
    override x.OnReceive (message:obj) =   
        match message with
        | :? ClientManagerInitMessage as msg ->
            originalSender <- x.Sender
            numClients <- msg.NumClients
            // high profile user count - 0.1 % of total users
            // medium profile user count - 5% of total users
            // low profile user count - rest of the users
            let highProfileUserEnd = int(0.001 * float(numClients))
            let midProfileUserEnd = int(0.05 * float(numClients))
            printfn "Total user count: %d" numClients
            printfn "High profile user count: %d" highProfileUserEnd
            printfn "Medium profile user count: %d" (midProfileUserEnd - highProfileUserEnd)
            printfn "Low profile user count: %d" (numClients - midProfileUserEnd)
            let mutable highProfileUserTweets = 0
            let mutable mediumProfileUserTweets = 0
            let mutable lowProfileUserTweets = 0
            for id in 1 .. numClients do // creating users here and giving tweet counts
                let random = new Random()
                let actor = system.ActorOf(Props(typedefof<Client>), string(id))
                let mutable numTweets = 0
                if id <= highProfileUserEnd then
                    numTweets <- random.Next(20) + 50
                    highProfileUserTweets <- highProfileUserTweets + numTweets
                else if id <= midProfileUserEnd then
                    numTweets <- random.Next(15) + 15
                    mediumProfileUserTweets <- mediumProfileUserTweets + numTweets
                else
                    numTweets <- random.Next(10)
                    lowProfileUserTweets <- lowProfileUserTweets + numTweets
                let mutable disconnect = false // 10% disconnect
                if (random.Next(10) = 3) then
                    disconnect <- true
                    disConnectedUsers <- disConnectedUsers + 1
                actor.Tell { MyId = id; MyTweetCount = numTweets; NumClients = numClients; Disconnect = disconnect; }
            totalTweets <- (highProfileUserTweets + mediumProfileUserTweets + lowProfileUserTweets)
            printfn "Total Disconnected Users: %d" disConnectedUsers
            printfn "Total tweets: %d" totalTweets
            printfn "High profile user tweets %d" highProfileUserTweets
            printfn "Medium profile user tweets %d" mediumProfileUserTweets
            printfn "Low profile user tweets %d" lowProfileUserTweets
            printfn "Waiting to complete ..."
            // all users initialized. create zipf follow pattern
            // high profile user follower count - 30 to 50% of total users
            // medium profile user follower count - 1 to 10% of total users
            // low profile user follower count - 0.05 to 0.1% of total users
            let mutable highProfileTotalFollowers = 0
            let mutable mediumProfileTotalFollowers = 0
            let mutable lowProfileTotalFollowers = 0
            for id in 1 .. numClients do
                let random = new Random()
                let mutable followersNeeded = 0
                if id <= highProfileUserEnd then
                    followersNeeded <- int(float(random.Next(200) + 300) * 0.001 * float(msg.NumClients))
                    highProfileTotalFollowers <- highProfileTotalFollowers + followersNeeded
                else if id <= midProfileUserEnd then
                    followersNeeded <- int(float(random.Next(900) + 100) * 0.0001 * float(msg.NumClients))
                    mediumProfileTotalFollowers <- mediumProfileTotalFollowers + followersNeeded
                else
                    followersNeeded <- int(float(random.Next(500) + 500) * 0.000001 * float(msg.NumClients))
                    lowProfileTotalFollowers <- lowProfileTotalFollowers + followersNeeded
                for count in 1 .. followersNeeded do
                    let newRandom = new Random()
                    let follower = newRandom.Next(1, msg.NumClients+1)
                    if id <> follower then
                        let followerRef = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:7887/user/"+string(follower))
                        followerRef.Tell { ForceFollowUserId = id; }
            printfn "Total followers for all users: %d" (highProfileTotalFollowers + mediumProfileTotalFollowers + lowProfileTotalFollowers)
            printfn "High profile total follower count: %d" highProfileTotalFollowers
            printfn "Medium profile total follower count: %d" mediumProfileTotalFollowers
            printfn "Low profile total follower count: %d" lowProfileTotalFollowers
            startTime <- System.DateTime.Now.TimeOfDay.TotalMilliseconds
            for id in 1 .. msg.NumClients do
                let myRef = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:7887/user/"+string(id))
                myRef.Tell { SimulateTweet = true;  }
        | :? ClientCompletionMessage as msg ->
            completedClients <- completedClients + 1
            if completedClients = numClients then
                printfn "Tweeting Completed."
                let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")
                server.Tell { Tweet = "FINISHED"; UserId = -1; IsRetweet=false}
                //originalSender.Tell { ClientManagerCompleted = true }
                //x.Self.Tell(PoisonPill.Instance)
        | :? Utils.AcknowledgementOfTweet as msg ->
            printfn "Completed"
            endTime <- System.DateTime.Now.TimeOfDay.TotalMilliseconds
            let timeTaken = (endTime - startTime)
            printfn "Time taken: %f" timeTaken
            printfn "Total original tweets: %d" totalTweets
            printfn "Total retweets: %d" retweetsCount
            printfn "Total tweets: %d" (totalTweets + retweetsCount)
            printfn "Tweets per second: %d" (int(float((retweetsCount+totalTweets)*1000)/timeTaken))
            originalSender.Tell { ClientManagerCompleted = true }
            x.Self.Tell(PoisonPill.Instance)
        | _ -> printfn "ERROR WHILE PARSING MESSAGE"

let server = system.ActorOf(Props(typedefof<ClientManager>), "ClientManager")
let (task:Async<ClientManagerCompletionMessage>) = ( server <? { NumClients = 1000; })
let response = Async.RunSynchronously (task)
server.Tell(PoisonPill.Instance)