// Common Interface for both client/simulator and server
type RegistrationConfirmation = {
    IsUserRegistered: bool;
}

type RegisterNewUser = {
    UserId: int;
}

type InactivateUser = {
    IsUserInactive: int;
}

type SubscribeUserAccount = {
    SelfId: int;
    UserId: int;
}

type GenerateTweet = {
    UserId: int;
    Tweet: string;
    IsRetweet: bool;
}

type AcknowledgementOfTweet = {
    IsTweetAcknowledged: bool;
    IsRetweet: bool;
}

type UpdateUserFeed = {
    FollowerId: int;
    Tweet: string;
    IsRetweet: bool;
}