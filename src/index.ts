import { PartitionAssigner, AssignerProtocol } from "kafkajs";
import { uniq, flatten } from "lodash";

export const MultiConsumerAssigner: PartitionAssigner = ({ cluster }) => ({
  name: "MultiConsumerAssigner",
  version: 1,
  protocol({ topics }) {
    return {
      name: this.name,
      metadata: AssignerProtocol.MemberMetadata.encode({
        version: this.version,
        topics,
        userData: Buffer.from(""),
      }),
    };
  },
  async assign({ members }) {
    const assignment: any = {};

    const memberTopics: Record<string, string[]> = {};

    members.forEach((member) => {
      const meta = AssignerProtocol.MemberMetadata.decode(
        member.memberMetadata
      )!;
      memberTopics[member.memberId] = meta.topics;
    });

    const topicMembers = Object.keys(memberTopics).reduce(
      (acc: Record<string, string[]>, memberId: string) => {
        memberTopics[memberId].forEach((topic) => {
          if (acc[topic]) {
            acc[topic].push(memberId);
          } else {
            acc[topic] = [memberId];
          }
        });

        return acc;
      },
      {}
    );
    Object.values(topicMembers).forEach((v) => v.sort());

    const consumedTopics = uniq(flatten(Object.values(memberTopics)));
    for (const topic of consumedTopics) {
      await cluster.addTargetTopic(topic);
    }

    const topicsPartionArrays = consumedTopics.map((topic) => {
      const partitionMetadata = cluster.findTopicPartitionMetadata(topic);

      return partitionMetadata.map((m) => ({
        topic: topic,
        partitionId: m.partitionId,
      }));
    });
    const topicsPartitions = flatten(topicsPartionArrays);

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
      memberAssignment: AssignerProtocol.MemberAssignment.encode({
        version: this.version,
        assignment: assignment[memberId],
        userData: Buffer.of(),
      }),
    }));
  },
});
