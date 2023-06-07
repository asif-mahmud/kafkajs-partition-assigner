"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MultiConsumerAssigner = void 0;
const kafkajs_1 = require("kafkajs");
const lodash_1 = require("lodash");
const MultiConsumerAssigner = ({ cluster }) => ({
    name: "MultiConsumerAssigner",
    version: 1,
    protocol({ topics }) {
        return {
            name: this.name,
            metadata: kafkajs_1.AssignerProtocol.MemberMetadata.encode({
                version: this.version,
                topics,
                userData: Buffer.from(""),
            }),
        };
    },
    assign({ members }) {
        return __awaiter(this, void 0, void 0, function* () {
            const assignment = {};
            const memberTopics = {};
            members.forEach((member) => {
                const meta = kafkajs_1.AssignerProtocol.MemberMetadata.decode(member.memberMetadata);
                memberTopics[member.memberId] = meta.topics;
            });
            const topicMembers = Object.keys(memberTopics).reduce((acc, memberId) => {
                memberTopics[memberId].forEach((topic) => {
                    if (acc[topic]) {
                        acc[topic].push(memberId);
                    }
                    else {
                        acc[topic] = [memberId];
                    }
                });
                return acc;
            }, {});
            Object.values(topicMembers).forEach((v) => v.sort());
            const consumedTopics = (0, lodash_1.uniq)((0, lodash_1.flatten)(Object.values(memberTopics)));
            for (const topic of consumedTopics) {
                yield cluster.addTargetTopic(topic);
            }
            const topicsPartionArrays = consumedTopics.map((topic) => {
                const partitionMetadata = cluster.findTopicPartitionMetadata(topic);
                return partitionMetadata.map((m) => ({
                    topic: topic,
                    partitionId: m.partitionId,
                }));
            });
            const topicsPartitions = (0, lodash_1.flatten)(topicsPartionArrays);
            topicsPartitions.forEach(({ topic, partitionId }, i) => {
                const assignee = topicMembers[topic][i % topicMembers[topic].length];
                if (!assignment[assignee]) {
                    assignment[assignee] = Object.create(null);
                }
                if (!assignment[assignee][topic]) {
                    assignment[assignee][topic] = [];
                }
                assignment[assignee][topic].push(partitionId);
            });
            return Object.keys(assignment).map((memberId) => ({
                memberId,
                memberAssignment: kafkajs_1.AssignerProtocol.MemberAssignment.encode({
                    version: this.version,
                    assignment: assignment[memberId],
                    userData: Buffer.of(),
                }),
            }));
        });
    },
});
exports.MultiConsumerAssigner = MultiConsumerAssigner;
