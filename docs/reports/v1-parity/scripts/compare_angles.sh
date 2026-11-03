
PROJECT_NAME="all_together"

grep -n "GridMovement\|PlaneNormal\|AreAnglesInverted" \
  /users/artem.kushner/dev/crboost_server/projects/${PROJECT_NAME}/External/job003/warp_tiltseries/${PROJECT_NAME}_Position_1.xml

echo "

"

grep -n "GridMovement\|PlaneNormal\|AreAnglesInverted" \
  /groups/klumpe/software/Setup/Testing/test1/run12/External/job003/warp_tiltseries/Position_1.xml
echo '

--------- 


'

grep -n "GridCTF" \
  /users/artem.kushner/dev/crboost_server/projects/${PROJECT_NAME}/External/job003/warp_tiltseries/${PROJECT_NAME}_Position_1.xml | head -5
echo "

"

grep -n "GridCTF" \
  /groups/klumpe/software/Setup/Testing/test1/run12/External/job003/warp_tiltseries/Position_1.xml | head -5
