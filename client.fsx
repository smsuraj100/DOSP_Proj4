#load "Utils.fsx"

#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open Utils
open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp

let args = System.Environment.GetCommandLineArgs()

printfn "args array -------------> %A" args

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
    NumClients: int;
} 

type InitSimulator = {
    MyId: int;
    MyTweetCount: int;
    NumClients: int;
    IsUserOffline: bool;
}

type HandleFinishedActorState = {
    ClientCompleted: bool;
}

type AddFollower = {
    FollowerId: int;
}

type FinishedClientEngineState = {
    IsClientEngineFinished: bool;
}

type SimulateTweet = {
    ShouldSimulateTweet: bool;
}

type SimulatorNode() = 
    inherit Actor()

    let mutable actorVal = 0
    let mutable noOfTweets = 0
    let mutable numClients = 0
    let mutable isUserOffline = false

    override x.OnReceive (message:obj) =
        match message with
        | :? InitSimulator as msg ->
            actorVal <- msg.MyId
            noOfTweets <- msg.MyTweetCount
            numClients <-msg.NumClients
            isUserOffline <- msg.IsUserOffline

            let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")
            
            // To register User
            server.Tell { UserId = actorVal; } 

        | :? AddFollower as msg ->
            let userId = msg.FollowerId
            let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")
            server.Tell { SelfId = actorVal; UserId = userId; }

        | :? Utils.RegistrationConfirmation as msg ->
            printfn "User%d successsfully registered !!!" actorVal

        | :? Utils.UpdateUserFeed as msg ->
            let nextRandNum = random.Next(10)
            if (nextRandNum = 6 && msg.IsRetweet = false) then
               noOfRetweets <- noOfRetweets + 1
               let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")
               server.Tell { UserId = actorVal; Tweet = msg.Tweet; IsRetweet = true }

        | :? Utils.AcknowledgementOfTweet as msg ->
            if msg.IsRetweet = false then
                x.Self.Tell { ShouldSimulateTweet = true }

        | :? SimulateTweet as msg ->
            // Simulate tweeting ifuser is active
            if noOfTweets <> 0 && isUserOffline = false then
                let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")

                let mutable tweet = ("tweet_" + string(noOfTweets))

                // Add mentions and hastags randomly
                if random.Next(2) = 1 then
                    tweet <- (tweet + "@" + string(random.Next(1, numClients + 1)))

                if random.Next(2) = 1 then
                    tweet <- (tweet + "#" + string(random.Next(1,10)))

                server.Tell { UserId = actorVal; Tweet = tweet; IsRetweet = false }

                noOfTweets <- noOfTweets - 1 
            else
                if isUserOffline = true then
                    let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")
                    server.Tell {IsUserInactive = actorVal}

                let clientManager = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:7887/user/ClientManager")

                clientManager.Tell { ClientCompleted = true }

        | _ -> printfn "Invalid Message !!!"
        
type ClientManager() =
    inherit Actor()

    let mutable sender = null
    let mutable numClients = 0
    let mutable noOfFinishedActors = 0
    let mutable startTime = 0.0
    let mutable endTime = 0.0
    let mutable totalTweetCount = 0

    override x.OnReceive (message:obj) =   
        match message with
        | :? InitClientEngineNode as msg ->
            sender <- x.Sender
            numClients <- msg.NumClients

            // No. of celebrities  - 0.5 % of total users
            let noOfCelebrities = int(0.005 * float(numClients))

            // No. of influencers - 5% of total users
            let noOfInfluencers = int(0.05 * float(numClients))

            printfn "--------------------------------------------------------------------"
            printfn "Total user count: %d" numClients
            printfn "No. of celebrities: %d" noOfCelebrities
            printfn "No. of influencers: %d" (noOfInfluencers - noOfCelebrities)
            printfn "No. of normal users: %d" (numClients - noOfInfluencers)
            printfn "--------------------------------------------------------------------"

            let mutable celebrityTweets = 0
            let mutable influencerTweets = 0
            let mutable normalUserTweets = 0

            for id in 1 .. numClients do              
                let actor = system.ActorOf(Props(typedefof<SimulatorNode>), string(id))

                let mutable numTweets = 0

                if id <= noOfCelebrities then
                    numTweets <- random.Next(20) + 40
                    celebrityTweets <- celebrityTweets + numTweets
                else if id <= noOfInfluencers then
                    numTweets <- random.Next(10) + 20
                    influencerTweets <- influencerTweets + numTweets
                else
                    numTweets <- random.Next(10)
                    normalUserTweets <- normalUserTweets + numTweets

                // Inactivate user (0 to 10% of total users)
                let mutable isUserOffline = false 

                if (random.Next(10) = 4) then
                    isUserOffline <- true
                    noOfInactiveUsers <- noOfInactiveUsers + 1

                actor.Tell { MyId = id; MyTweetCount = numTweets; NumClients = numClients; IsUserOffline = isUserOffline; }

            totalTweetCount <- (celebrityTweets + influencerTweets + normalUserTweets)

            printfn "--------------------------------------------------------------------"
            printfn "Total Inactive Users: %d" noOfInactiveUsers
            printfn "Total no. of tweets: %d" totalTweetCount
            printfn "No. of celebrity's tweets %d" celebrityTweets
            printfn "No. of influencers's tweets %d" influencerTweets
            printfn "No. of normal user's tweets %d" normalUserTweets
            printfn "--------------------------------------------------------------------"

            // To reate zipf follow pattern
            // celebrity's follower count - 40 to 50% of total users
            // influencer's follower count - 8 to 10% of total users
            // normal user's follower count - 1 to 3% of total users
            let mutable celebrityFollowerCount = 0
            let mutable influencerFollowerCount = 0
            let mutable normalUserFollowerCount = 0

            for id in 1 .. numClients do
                let mutable noOfFollowersAssigned = 0

                if id <= noOfCelebrities then
                    noOfFollowersAssigned <- int(float(random.Next(200) + 400) * 0.001 * float(msg.NumClients))
                    celebrityFollowerCount <- celebrityFollowerCount + noOfFollowersAssigned
                else if id <= noOfInfluencers then
                    noOfFollowersAssigned <- int(float(random.Next(200) + 800) * 0.0001 * float(msg.NumClients))
                    influencerFollowerCount <- influencerFollowerCount + noOfFollowersAssigned
                else
                    noOfFollowersAssigned <- int(float(random.Next(200) + 100) * 0.0001 * float(msg.NumClients))
                    normalUserFollowerCount <- normalUserFollowerCount + noOfFollowersAssigned

                for ele in 1 .. noOfFollowersAssigned do
                    let newRandom = new Random()
                    let follower = newRandom.Next(1, msg.NumClients+1)
                    if id <> follower then
                        let followerRef = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:7887/user/"+string(follower))
                        followerRef.Tell { FollowerId = id; }

            printfn "--------------------------------------------------------------------"
            printfn "Total no. of followers for all users: %d" (celebrityFollowerCount + influencerFollowerCount + normalUserFollowerCount)
            printfn "Total no. of followers of all celebrities: %d" celebrityFollowerCount
            printfn "Total no. of followers of all influencers: %d" influencerFollowerCount
            printfn "Total no. of followers of all normal users: %d" normalUserFollowerCount
            printfn "--------------------------------------------------------------------"

            startTime <- System.DateTime.Now.TimeOfDay.TotalMilliseconds

            for id in 1 .. msg.NumClients do
                let selfActorRef = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:7887/user/"+string(id))
                selfActorRef.Tell { ShouldSimulateTweet = true;  }

        | :? HandleFinishedActorState as msg ->
            noOfFinishedActors <- noOfFinishedActors + 1
            if noOfFinishedActors = numClients then
                let server = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:8778/user/Server")
                server.Tell { Tweet = "FINISHED"; UserId = -1; IsRetweet = false}

        | :? Utils.AcknowledgementOfTweet as msg ->
            endTime <- System.DateTime.Now.TimeOfDay.TotalMilliseconds
            let timeTaken = (endTime - startTime)

            printfn "--------------------------------------------------------------------"
            printfn "Time taken: %f" timeTaken
            printfn "No. of original tweets: %d" totalTweetCount
            printfn "No. of retweets: %d" noOfRetweets
            printfn "Total tweets: %d" (totalTweetCount + noOfRetweets)
            printfn "Tweets per second: %d" (int(float((noOfRetweets+totalTweetCount)*1000)/timeTaken))
            printfn "--------------------------------------------------------------------"

            sender.Tell { IsClientEngineFinished = true }

            x.Self.Tell(PoisonPill.Instance)

        | _ -> printfn "Invalid Message !!!"

let server = system.ActorOf(Props(typedefof<ClientManager>), "ClientManager")
let (task:Async<FinishedClientEngineState>) = ( server <? { NumClients = args.[2] |> int; })
let response = Async.RunSynchronously (task)
server.Tell(PoisonPill.Instance)