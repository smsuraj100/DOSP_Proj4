#load "Utils.fsx"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open Utils
open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.Remote
open System.Collections.Generic

let args = System.Environment.GetCommandLineArgs()

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {            
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""                
            }
            remote {
                helios.tcp {
                    port = 8778
                    hostname = localhost
                }
            }
        }")

let system = ActorSystem.Create("RemoteFSharp", configuration)

type InitServer = {
    IsServerInitialized: bool;
} 

type FinishedServer = {
    isServerFinished: bool;
}

type Server() =
    inherit Actor()

    let mutable noOfTweets = 0
    let mutable noOfRetweets = 0
    let mutable sender = null

    let listOfRegisteredUsers = Dictionary<int, int>()
    let listOfMentions = Dictionary<string, List<string>>()
    let listOfHashtags = Dictionary<string, List<string>>()
    let listOfFollowers = Dictionary<int, List<int>>()

    override x.OnReceive (message:obj) =   
        match message with
        | :? InitServer as msg ->
            printfn "Initialized the server"
            sender <- x.Sender

        | :? Utils.RegisterNewUser as msg ->
            listOfRegisteredUsers.Add(msg.UserId, 1)
            let mutable tempList = new List<int>()
            listOfFollowers.Add(msg.UserId, tempList)

        | :? Utils.InactivateUser as msg ->
            listOfRegisteredUsers.Item(msg.IsUserInactive) <- 0

        | :? Utils.SubscribeUserAccount as msg ->
            let selfId = msg.SelfId
            listOfFollowers.Item(selfId).Add(msg.UserId)

        | :? Utils.GenerateTweet as msg ->   
            let tweet = msg.Tweet
            let userId = msg.UserId
            let isRetweet = msg.IsRetweet

            if isRetweet = true then
                noOfRetweets <- noOfRetweets + 1
            else
                noOfTweets <- noOfTweets + 1

            if tweet <> "FINISHED" then
                if tweet.Contains("#") && tweet.Contains("@") then                
                    // Add hastags in list of hastag
                    let currentHashtag = (tweet.Split "#").[1]

                    if not(listOfHashtags.ContainsKey(currentHashtag)) then
                        let mutable tempList = new List<string>()
                        listOfHashtags.Add(currentHashtag, tempList)

                    listOfHashtags.Item(currentHashtag).Add(tweet)

                    // Add mentions in mention list
                    let currentMention = ((tweet.Split "#").[0].Split "@").[1]

                    if not(listOfMentions.ContainsKey(currentMention)) then
                        let mutable tempList = new List<string>()
                        listOfMentions.Add(currentMention, tempList)

                    listOfMentions.Item(currentMention).Add(tweet)
                else if tweet.Contains("@") then // only mentions
                    let currentMention = (tweet.Split "@").[1]

                    if not(listOfMentions.ContainsKey(currentMention)) then
                        let mutable tempList = new List<string>()
                        listOfMentions.Add(currentMention, tempList)
                    listOfMentions.Item(currentMention).Add(tweet)  
                else if tweet.Contains("#") then // only hashtags
                    let currentHashtag = (tweet.Split "#").[1]

                    if not(listOfHashtags.ContainsKey(currentHashtag)) then
                        let mutable tempList = new List<string>()
                        listOfHashtags.Add(currentHashtag, tempList)
                    listOfHashtags.Item(currentHashtag).Add(tweet)

                for follower in listOfFollowers.Item(userId) do
                    if listOfRegisteredUsers.Item(follower) = 1 then
                        let actorRef = system.ActorSelection("akka.tcp://RemoteFSharp@localhost:7887/user/" + string(follower))
                        actorRef.Tell {FollowerId = userId; Tweet = tweet; IsRetweet = isRetweet }

                x.Sender.Tell { IsTweetAcknowledged = true; IsRetweet = isRetweet; }
            else
                x.Sender.Tell { IsTweetAcknowledged = true; IsRetweet = isRetweet; }
                sender.Tell { isServerFinished = true }
                printfn "Finished."

        | _ -> printfn "Invalid Message"

let server = system.ActorOf(Props(typedefof<Server>), "Server")
let (task:Async<FinishedServer>) = ( server <? { IsServerInitialized = true; })
let response = Async.RunSynchronously (task)
server.Tell(PoisonPill.Instance);