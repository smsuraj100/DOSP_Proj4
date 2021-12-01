#load "Utils.fsx"

#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open Utils
open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp

let args = System.Environment.GetCommandLineArgs()

printf "args:---------------> %A" args

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {            
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""                
            }
            remote {
                helios.tcp {
                    port = 7887
                    hostname = localhost
                }
            }
        }")

let system = ActorSystem.Create("RemoteFSharp", configuration)

let mutable noOfRetweets = 0
let mutable noOfInactiveUsers = 0
let random = Random()

type InitClientEngineNode = {
    NoOfClients: int;
} 

type InitSimulator = {
    SelfId: int;
    NoOfClients: int;
    IsInactive: bool;
    NoOfTweets: int;
}

type HandleFinishedActorState = {
    IsActorFinished: bool;
}

type AddFollower = {
    UserId: int;
}

type FinishedClientEngineState = {
    IsClientEngineFinished: bool;
}

type SimulateTweet = {
    ShouldSimulateTweet: bool;
}

type PopularityTeller = {
    Popularity: string;
}

type SimulatorNode() = 
    inherit Actor()

    let mutable selfId = 0
    let mutable noOfTweets = 0
    let mutable noOfClients = 0    
    let mutable isUserOffline = false

    override x.OnReceive (message:obj) =
        match message with
        | :? InitSimulator as msg ->
            selfId <- msg.SelfId
            noOfTweets <- msg.NoOfTweets
            noOfClients <-msg.NoOfClients
            isUserOffline <- msg.IsInactive

            // To register User
            let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")
            server.Tell { UserId = selfId; } 

        | :? AddFollower as msg ->
            let userId = msg.UserId
            let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")
            server.Tell { SelfId = selfId; UserId = userId; }

        | :? Utils.RegistrationConfirmation as msg ->
            printfn "User%d successsfully registered !!!" selfId

        | :? Utils.UpdateUserFeed as msg ->            
            let nextRandNum = random.Next(10)
            if (nextRandNum = 6 && msg.IsRetweet = false) then
               noOfRetweets <- noOfRetweets + 1
               let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")
               server.Tell { UserId = selfId; Tweet = msg.Tweet; IsRetweet = true }

        | :? Utils.AcknowledgementOfTweet as msg ->
            if msg.IsRetweet = false then
                x.Self.Tell { ShouldSimulateTweet = true }

        | :? SimulateTweet as msg ->
            // Simulate tweeting ifuser is active
            if noOfTweets <> 0 && isUserOffline = false then
                let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")

                let mutable tweet = ("RandomTweet_" + string(noOfTweets))
                // Add mentions and hastags randomly
                if random.Next(2) = 0 then
                    tweet <- (tweet + "@" + string(random.Next(1, noOfClients + 1)))

                if random.Next(2) = 1 then
                    tweet <- (tweet + "#" + string(random.Next(1, 38)))

                server.Tell { UserId = selfId; Tweet = tweet; IsRetweet=false }

                noOfTweets <- noOfTweets - 1 
            else
                if isUserOffline = true then
                    let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")
                    server.Tell {IsUserInactive = selfId}

                let clientManager = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:7887/user/ClientEngineNode")
                clientManager.Tell { IsActorFinished = true }

        | _ -> printfn "Invalid Message !!!"
        
type ClientEngineNode() =
    inherit Actor()

    let mutable sender = null
    let mutable noOfClients = 0
    let mutable noOfFinishedActors = 0
    let mutable beginTime = 0.0
    let mutable endTime = 0.0
    let mutable totalTweetCount = 0

    override x.OnReceive (message:obj) =   
        match message with
        | :? InitClientEngineNode as msg ->
            sender <- x.Sender
            noOfClients <- msg.NoOfClients

            // high profile user count - 0.1 % of total users
            // medium profile user count - 5% of total users
            // low profile user count - rest of the users
            let noOfCelebrities = int(0.001 * float(noOfClients))
            let noOfInfluencers = int(0.05 * float(noOfClients))

            printfn "Total user count: %d" noOfClients
            printfn "High profile user count: %d" noOfCelebrities
            printfn "Medium profile user count: %d" (noOfInfluencers - noOfCelebrities)
            printfn "Low profile user count: %d" (noOfClients - noOfInfluencers)

            let mutable celebrityTweets = 0
            let mutable influencerTweets = 0
            let mutable normalUserTweets = 0

            // To assign tweets according to role and randomly inactivate users
            for i in 1 .. noOfClients do 
                let actor = system.ActorOf(Props(typedefof<SimulatorNode>), string(id))

                printfn "Inside for loop - 1"
                let mutable noOfTweetsAssigned = 0

                if i <= noOfCelebrities then
                    noOfTweetsAssigned <- random.Next(20) + 40
                    celebrityTweets <- celebrityTweets + noOfTweetsAssigned
                else if i <= noOfInfluencers then
                    noOfTweetsAssigned <- random.Next(10) + 20
                    influencerTweets <- influencerTweets + noOfTweetsAssigned
                else
                    noOfTweetsAssigned <- random.Next(15)
                    normalUserTweets <- normalUserTweets + noOfTweetsAssigned

                printfn "Inside for loop - 2"
                let mutable isInactive = false // 10% isInactive
                if (random.Next(10) = 4) then
                    isInactive <- true
                    noOfInactiveUsers <- noOfInactiveUsers + 1
                printfn "Inside for loop - 3"
                actor.Tell { SelfId = i; NoOfTweets = noOfTweetsAssigned; NoOfClients = noOfClients; IsInactive = isInactive; }
                printfn "Inside for loop - 4"

            printfn "Outside for loop - 1"
            totalTweetCount <- (celebrityTweets + influencerTweets + normalUserTweets)
            
            
            printfn "Total Inactive/Offline Users: %d" noOfInactiveUsers
            printfn "Total no. of tweets: %d" totalTweetCount
            printfn "No. of tweets by a celebrity: %d" celebrityTweets
            printfn "No. of tweets by an influencer: %d" influencerTweets
            printfn "No. of tweets by a normal user: %d" normalUserTweets

            // all users initialized. create zipf follow pattern
            // high profile user follower count - 30 to 50% of total users
            // medium profile user follower count - 1 to 10% of total users
            // low profile user follower count - 0.05 to 0.1% of total users
            let mutable celebrityFollowerCount = 0
            let mutable influencerFollowerCount = 0
            let mutable normalUserFollowerCount = 0
            printfn "Outside for loop - 2"
            for i in 1 .. noOfClients do
                let mutable noOfFollowersAssigned = 0

                if i <= noOfCelebrities then
                    noOfFollowersAssigned <- int(float(random.Next(200) + 300) * 0.001 * float(msg.NoOfClients))
                    celebrityFollowerCount <- celebrityFollowerCount + noOfFollowersAssigned
                else if i <= noOfInfluencers then
                    noOfFollowersAssigned <- int(float(random.Next(900) + 100) * 0.0001 * float(msg.NoOfClients))
                    influencerFollowerCount <- influencerFollowerCount + noOfFollowersAssigned
                else
                    noOfFollowersAssigned <- int(float(random.Next(500) + 500) * 0.000001 * float(msg.NoOfClients))
                    normalUserFollowerCount <- normalUserFollowerCount + noOfFollowersAssigned

                for count in 1 .. noOfFollowersAssigned do
                    let followerId = random.Next(1, msg.NoOfClients + 1)
                    if i <> followerId then
                        let followerRef = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:7887/user/" + string(followerId))
                        followerRef.Tell { UserId = i; }

            printfn "Celebrity follower count: %d" celebrityFollowerCount
            printfn "Influencer follower count: %d" influencerFollowerCount
            printfn "Normal user follower count: %d" normalUserFollowerCount

            beginTime <- System.DateTime.Now.TimeOfDay.TotalMilliseconds

            for i in 1 .. msg.NoOfClients do
                let selfActorRef = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:7887/user/" + string(i))
                selfActorRef.Tell { ShouldSimulateTweet = true;  }

        | :? HandleFinishedActorState as msg ->
            noOfFinishedActors <- noOfFinishedActors + 1
            if noOfFinishedActors = noOfClients then
                let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")
                server.Tell { Tweet = "FINISHED"; UserId = -1; IsRetweet = false}

        | :? Utils.AcknowledgementOfTweet as msg ->
            printfn "Finishedd !!"
            endTime <- System.DateTime.Now.TimeOfDay.TotalMilliseconds
            let timeElapsed = (endTime - beginTime)
            printfn "Total time elapsed: %f" timeElapsed
            printfn "Total no. of original tweets: %d" totalTweetCount
            printfn "Total no. of retweets: %d" noOfRetweets
            printfn "Total tweets: %d" (totalTweetCount + noOfRetweets)
            printfn "Tweets per second: %d" (int(float((noOfRetweets+totalTweetCount)*1000)/timeElapsed))
            sender.Tell { IsClientEngineFinished = true }
            x.Self.Tell(PoisonPill.Instance)
            
        | _ -> printfn "ERROR WHILE PARSING MESSAGE"

let server = system.ActorOf(Props(typedefof<ClientEngineNode>), "ClientEngineNode")
let (task:Async<FinishedClientEngineState>) = ( server <? { NoOfClients = 1000; })
let response = Async.RunSynchronously (task)
server.Tell(PoisonPill.Instance)